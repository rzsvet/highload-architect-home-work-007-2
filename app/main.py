"""
Микросервис диалогов с поддержкой Prometheus метрик
"""

from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from contextlib import asynccontextmanager
import redis
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Tuple
import json
from datetime import datetime, timedelta
import logging
import uuid
import time
import psutil
import threading
from collections import defaultdict
import os

# Prometheus импорты
from prometheus_client import Counter, Histogram, Gauge, Summary, Info, generate_latest
from prometheus_client import CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client.metrics import MetricWrapperBase

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# МОДЕЛИ ДАННЫХ
# ============================================================================

class Message(BaseModel):
    """Модель сообщения в диалоге"""
    role: str = Field(..., description="Роль отправителя: user, assistant, system")
    content: str = Field(..., description="Текст сообщения")
    timestamp: Optional[str] = Field(None, description="Временная метка сообщения")
    
    class Config:
        json_schema_extra = {
            "example": {
                "role": "user",
                "content": "Привет! Как дела?",
                "timestamp": "2024-01-15T10:30:00"
            }
        }

class Dialog(BaseModel):
    """Модель диалога"""
    dialog_id: str = Field(..., description="Уникальный идентификатор диалога")
    user_id: str = Field(..., description="Идентификатор пользователя")
    messages: List[Message] = Field(default_factory=list, description="Список сообщений")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Метаданные диалога")
    created_at: Optional[str] = Field(None, description="Время создания")
    updated_at: Optional[str] = Field(None, description="Время последнего обновления")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dialog_id": "123e4567-e89b-12d3-a456-426614174000",
                "user_id": "user_123",
                "messages": [
                    {
                        "role": "user",
                        "content": "Привет!",
                        "timestamp": "2024-01-15T10:30:00"
                    }
                ],
                "metadata": {"source": "web"},
                "created_at": "2024-01-15T10:30:00",
                "updated_at": "2024-01-15T10:30:00"
            }
        }

class CreateDialogRequest(BaseModel):
    """Запрос на создание диалога"""
    user_id: str = Field(..., description="Идентификатор пользователя")
    initial_message: Optional[Message] = Field(None, description="Первое сообщение в диалоге")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Метаданные диалога")
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "user_123",
                "initial_message": {
                    "role": "user",
                    "content": "Привет! Как дела?"
                },
                "metadata": {"source": "web", "platform": "mobile"}
            }
        }

class AddMessageRequest(BaseModel):
    """Запрос на добавление сообщения"""
    message: Message = Field(..., description="Сообщение для добавления")
    
    class Config:
        json_schema_extra = {
            "example": {
                "message": {
                    "role": "assistant",
                    "content": "Здравствуйте! Чем могу помочь?"
                }
            }
        }

class CompleteDialogRequest(BaseModel):
    """Запрос на завершение диалога"""
    dialog_id: str = Field(..., description="Идентификатор диалога")
    completion_reason: Optional[str] = Field("completed", description="Причина завершения")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dialog_id": "123e4567-e89b-12d3-a456-426614174000",
                "completion_reason": "resolved"
            }
        }

class SearchMessagesRequest(BaseModel):
    """Запрос на поиск сообщений"""
    search_text: str = Field(..., description="Текст для поиска")
    limit: Optional[int] = Field(10, description="Лимит результатов")
    
    class Config:
        json_schema_extra = {
            "example": {
                "search_text": "помощь",
                "limit": 10
            }
        }

# ============================================================================
# LUA СКРИПТЫ ДЛЯ REDIS
# ============================================================================

LUA_SCRIPTS = {
    "create_dialog": """
        local dialog_id = ARGV[1]
        local user_id = ARGV[2]
        local metadata = ARGV[3]
        local initial_message = ARGV[4]
        local timestamp = ARGV[5]
        
        -- Проверяем существование диалога
        if redis.call('EXISTS', 'dialog:' .. dialog_id) == 1 then
            return redis.error_reply('Dialog already exists')
        end
        
        -- Создаем диалог
        redis.call('HSET', 'dialog:' .. dialog_id,
            'user_id', user_id,
            'metadata', metadata,
            'created_at', timestamp,
            'updated_at', timestamp
        )
        
        -- Добавляем в список диалогов пользователя
        redis.call('SADD', 'user_dialogs:' .. user_id, dialog_id)
        
        -- Если есть начальное сообщение, добавляем его
        if initial_message ~= '' and initial_message ~= 'null' then
            redis.call('RPUSH', 'dialog_messages:' .. dialog_id, initial_message)
        end
        
        return dialog_id
    """,
    
    "add_message": """
        local dialog_id = ARGV[1]
        local message = ARGV[2]
        local timestamp = ARGV[3]
        
        -- Проверяем существование диалога
        if redis.call('EXISTS', 'dialog:' .. dialog_id) == 0 then
            return redis.error_reply('Dialog not found')
        end
        
        -- Добавляем сообщение
        redis.call('RPUSH', 'dialog_messages:' .. dialog_id, message)
        
        -- Обновляем время последнего изменения
        redis.call('HSET', 'dialog:' .. dialog_id, 'updated_at', timestamp)
        
        -- Получаем общее количество сообщений
        local message_count = redis.call('LLEN', 'dialog_messages:' .. dialog_id)
        
        return message_count
    """,
    
    "get_dialog_with_messages": """
        local dialog_id = ARGV[1]
        local start_idx = tonumber(ARGV[2])
        local end_idx = tonumber(ARGV[3])
        
        -- Получаем данные диалога
        local dialog_data = redis.call('HGETALL', 'dialog:' .. dialog_id)
        if #dialog_data == 0 then
            return redis.error_reply('Dialog not found')
        end
        
        -- Преобразуем в таблицу
        local dialog = {}
        for i = 1, #dialog_data, 2 do
            dialog[dialog_data[i]] = dialog_data[i + 1]
        end
        
        -- Получаем сообщения
        local messages = {}
        if start_idx and end_idx then
            messages = redis.call('LRANGE', 'dialog_messages:' .. dialog_id, start_idx, end_idx)
        else
            messages = redis.call('LRANGE', 'dialog_messages:' .. dialog_id, 0, -1)
        end
        
        -- Парсим сообщения из JSON
        local parsed_messages = {}
        for i, msg in ipairs(messages) do
            local success, parsed = pcall(cjson.decode, msg)
            if success then
                parsed_messages[i] = parsed
            else
                parsed_messages[i] = {error = 'Failed to parse message', raw = string.sub(msg, 1, 100)}
            end
        end
        
        -- Возвращаем как таблицу
        return {dialog, parsed_messages}
    """,
    
    "get_user_dialogs": """
        local user_id = ARGV[1]
        local limit = tonumber(ARGV[2])
        local offset = tonumber(ARGV[3])
        
        -- Получаем список диалогов пользователя
        local dialog_ids = redis.call('SMEMBERS', 'user_dialogs:' .. user_id)
        
        if #dialog_ids == 0 then
            return '[]'
        end
        
        -- Получаем время обновления для каждого диалога
        local dialogs_with_info = {}
        for i, dialog_id in ipairs(dialog_ids) do
            local updated_at = redis.call('HGET', 'dialog:' .. dialog_id, 'updated_at') or ''
            local created_at = redis.call('HGET', 'dialog:' .. dialog_id, 'created_at') or ''
            local metadata = redis.call('HGET', 'dialog:' .. dialog_id, 'metadata') or '{}'
            
            dialogs_with_info[i] = {
                dialog_id = dialog_id,
                updated_at = updated_at,
                created_at = created_at,
                metadata = metadata
            }
        end
        
        -- Сортируем по времени обновления (последние сначала)
        table.sort(dialogs_with_info, function(a, b)
            return a.updated_at > b.updated_at
        end)
        
        -- Применяем пагинацию
        local result = {}
        local start = offset + 1
        local finish = math.min(start + limit - 1, #dialogs_with_info)
        
        for i = start, finish do
            local dialog_info = dialogs_with_info[i]
            
            -- Получаем последнее сообщение
            local last_message_json = redis.call('LINDEX', 'dialog_messages:' .. dialog_info.dialog_id, -1)
            local last_message = nil
            if last_message_json then
                local success, parsed = pcall(cjson.decode, last_message_json)
                if success then
                    last_message = parsed
                end
            end
            
            result[#result + 1] = {
                dialog_id = dialog_info.dialog_id,
                metadata = dialog_info.metadata,
                created_at = dialog_info.created_at,
                updated_at = dialog_info.updated_at,
                last_message = last_message
            }
        end
        
        return cjson.encode(result)
    """,
    
    "delete_dialog": """
        local dialog_id = ARGV[1]
        
        -- Получаем user_id перед удалением
        local user_id = redis.call('HGET', 'dialog:' .. dialog_id, 'user_id')
        
        -- Удаляем диалог
        redis.call('DEL', 'dialog:' .. dialog_id)
        redis.call('DEL', 'dialog_messages:' .. dialog_id)
        
        -- Удаляем из списка диалогов пользователя
        if user_id then
            redis.call('SREM', 'user_dialogs:' .. user_id, dialog_id)
        end
        
        return 'deleted'
    """,
    
    "search_messages": """
        local dialog_id = ARGV[1]
        local search_text = string.lower(ARGV[2])
        local limit = tonumber(ARGV[3])
        
        local all_messages = redis.call('LRANGE', 'dialog_messages:' .. dialog_id, 0, -1)
        local results = {}
        local count = 0
        
        for i, msg_json in ipairs(all_messages) do
            local success, msg = pcall(cjson.decode, msg_json)
            if success and msg.content then
                local content = string.lower(msg.content)
                
                if string.find(content, search_text) then
                    results[#results + 1] = {
                        index = i - 1,
                        message = msg
                    }
                    count = count + 1
                    
                    if limit and count >= limit then
                        break
                    end
                end
            end
        end
        
        return cjson.encode(results)
    """,
    
    "get_dialog_stats": """
        local dialog_id = ARGV[1]
        
        -- Получаем все сообщения
        local messages = redis.call('LRANGE', 'dialog_messages:' .. dialog_id, 0, -1)
        
        local stats = {
            total_messages = #messages,
            user_messages = 0,
            assistant_messages = 0,
            system_messages = 0,
            total_chars = 0,
            first_message_time = nil,
            last_message_time = nil
        }
        
        for i, msg_json in ipairs(messages) do
            local success, msg = pcall(cjson.decode, msg_json)
            if success then
                if msg.role == 'user' then
                    stats.user_messages = stats.user_messages + 1
                elseif msg.role == 'assistant' then
                    stats.assistant_messages = stats.assistant_messages + 1
                elseif msg.role == 'system' then
                    stats.system_messages = stats.system_messages + 1
                end
                
                if msg.content then
                    stats.total_chars = stats.total_chars + string.len(msg.content)
                end
                
                if msg.timestamp then
                    if not stats.first_message_time or msg.timestamp < stats.first_message_time then
                        stats.first_message_time = msg.timestamp
                    end
                    if not stats.last_message_time or msg.timestamp > stats.last_message_time then
                        stats.last_message_time = msg.timestamp
                    end
                end
            end
        end
        
        if stats.total_messages > 0 then
            stats.avg_message_length = stats.total_chars / stats.total_messages
        else
            stats.avg_message_length = 0
        end
        
        return cjson.encode(stats)
    """,
    
    "cleanup_old_dialogs": """
        local older_than_days = tonumber(ARGV[1])
        local timestamp = ARGV[2]
        local limit = tonumber(ARGV[3]) or 100
        
        local cutoff_time = os.time() - (older_than_days * 86400)
        local deleted_count = 0
        
        -- Ищем старые диалоги
        local cursor = 0
        repeat
            local result = redis.call('SCAN', cursor, 'MATCH', 'dialog:*', 'COUNT', 100)
            cursor = tonumber(result[1])
            local keys = result[2]
            
            for _, key in ipairs(keys) do
                if deleted_count >= limit then
                    break
                end
                
                local updated_at = redis.call('HGET', key, 'updated_at')
                if updated_at then
                    -- Парсим ISO timestamp
                    local year, month, day, hour, min, sec = 
                        string.match(updated_at, '(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)')
                    if year then
                        local last_active = os.time{
                            year=tonumber(year), 
                            month=tonumber(month), 
                            day=tonumber(day),
                            hour=tonumber(hour), 
                            min=tonumber(min), 
                            sec=tonumber(sec)
                        }
                        
                        if last_active < cutoff_time then
                            local dialog_id = string.sub(key, 8)  -- удаляем 'dialog:' префикс
                            local user_id = redis.call('HGET', key, 'user_id')
                            
                            -- Удаляем диалог
                            redis.call('DEL', key)
                            redis.call('DEL', 'dialog_messages:' .. dialog_id)
                            
                            -- Удаляем из списка диалогов пользователя
                            if user_id then
                                redis.call('SREM', 'user_dialogs:' .. user_id, dialog_id)
                            end
                            
                            deleted_count = deleted_count + 1
                        end
                    end
                end
            end
        until cursor == 0 or deleted_count >= limit
        
        return deleted_count
    """
}

# ============================================================================
# МЕТРИКИ PROMETHEUS
# ============================================================================

class DialogMetricsCollector:
    """Коллектор метрик для микросервиса диалогов"""
    
    def __init__(self):
        self.registry = CollectorRegistry()
        
        # === Базовые метрики сервиса ===
        self.service_info = Info(
            'dialog_service_info',
            'Information about dialog service',
            registry=self.registry
        )
        
        # === API метрики ===
        self.api_requests_total = Counter(
            'dialog_api_requests_total',
            'Total number of API requests',
            ['method', 'endpoint', 'status'],
            registry=self.registry
        )
        
        self.api_request_duration_seconds = Histogram(
            'dialog_api_request_duration_seconds',
            'API request duration in seconds',
            ['method', 'endpoint'],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
            registry=self.registry
        )
        
        self.api_request_size_bytes = Histogram(
            'dialog_api_request_size_bytes',
            'API request size in bytes',
            ['method', 'endpoint'],
            buckets=[100, 500, 1000, 5000, 10000, 50000, 100000],
            registry=self.registry
        )
        
        self.api_response_size_bytes = Histogram(
            'dialog_api_response_size_bytes',
            'API response size in bytes',
            ['method', 'endpoint'],
            buckets=[100, 500, 1000, 5000, 10000, 50000, 100000, 500000],
            registry=self.registry
        )
        
        # === Бизнес метрики ===
        self.dialogs_created_total = Counter(
            'dialog_dialogs_created_total',
            'Total number of dialogs created',
            ['source', 'platform'],
            registry=self.registry
        )
        
        self.messages_sent_total = Counter(
            'dialog_messages_sent_total',
            'Total number of messages sent',
            ['role'],
            registry=self.registry
        )
        
        self.active_dialogs = Gauge(
            'dialog_active_dialogs',
            'Number of currently active dialogs',
            registry=self.registry
        )
        
        self.active_users = Gauge(
            'dialog_active_users',
            'Number of currently active users',
            registry=self.registry
        )
        
        self.dialog_duration_seconds = Histogram(
            'dialog_dialog_duration_seconds',
            'Duration of dialogs in seconds',
            buckets=[60, 300, 600, 1800, 3600, 7200, 14400, 28800],
            registry=self.registry
        )
        
        self.messages_per_dialog = Histogram(
            'dialog_messages_per_dialog',
            'Number of messages per dialog',
            buckets=[1, 3, 5, 10, 20, 50, 100],
            registry=self.registry
        )
        
        self.message_length_chars = Histogram(
            'dialog_message_length_chars',
            'Length of messages in characters',
            buckets=[10, 50, 100, 200, 500, 1000, 2000],
            registry=self.registry
        )
        
        self.assistant_response_time_seconds = Histogram(
            'dialog_assistant_response_time_seconds',
            'Assistant response time in seconds',
            buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60],
            registry=self.registry
        )
        
        # === Системные метрики ===
        self.redis_operations_total = Counter(
            'dialog_redis_operations_total',
            'Total number of Redis operations',
            ['operation'],
            registry=self.registry
        )
        
        self.redis_operation_duration_seconds = Histogram(
            'dialog_redis_operation_duration_seconds',
            'Redis operation duration in seconds',
            ['operation'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
            registry=self.registry
        )
        
        self.service_uptime_seconds = Gauge(
            'dialog_service_uptime_seconds',
            'Service uptime in seconds',
            registry=self.registry
        )
        
        self.memory_usage_bytes = Gauge(
            'dialog_memory_usage_bytes',
            'Memory usage of the service in bytes',
            registry=self.registry
        )
        
        # === Метрики ошибок ===
        self.errors_total = Counter(
            'dialog_errors_total',
            'Total number of errors',
            ['type', 'source'],
            registry=self.registry
        )
        
        # === Дополнительные бизнес метрики ===
        self.user_retention_rate = Gauge(
            'dialog_user_retention_rate',
            'User retention rate (7-day)',
            registry=self.registry
        )
        
        self.avg_messages_per_user = Gauge(
            'dialog_avg_messages_per_user',
            'Average number of messages per user',
            registry=self.registry
        )
        
        self.dialog_completion_rate = Gauge(
            'dialog_dialog_completion_rate',
            'Dialog completion rate',
            registry=self.registry
        )
        
        # Инициализация
        self.service_start_time = time.time()
        self.service_info.info({
            'version': '2.0.0',
            'environment': 'production',
            'service_name': 'dialog-microservice'
        })
        
        # Внутренние структуры для вычислений
        self.user_activity = defaultdict(list)
        self.dialog_metrics_cache = {}
    
    def record_api_request(self, method: str, endpoint: str, status_code: int, 
                          duration: float, request_size: int = 0, response_size: int = 0):
        """Запись метрик API запроса"""
        status = f"{status_code // 100}xx"  # Группируем по классам
        
        self.api_requests_total.labels(
            method=method,
            endpoint=endpoint,
            status=status
        ).inc()
        
        self.api_request_duration_seconds.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
        
        if request_size > 0:
            self.api_request_size_bytes.labels(
                method=method,
                endpoint=endpoint
            ).observe(request_size)
        
        if response_size > 0:
            self.api_response_size_bytes.labels(
                method=method,
                endpoint=endpoint
            ).observe(response_size)
    
    def record_dialog_created(self, user_id: str, metadata: Dict):
        """Запись метрик создания диалога"""
        source = metadata.get('source', 'unknown')
        platform = metadata.get('platform', 'unknown')
        
        self.dialogs_created_total.labels(
            source=source,
            platform=platform
        ).inc()
        
        # Обновляем активных пользователей
        self.update_user_activity(user_id)
    
    def record_message_sent(self, role: str, content: str, dialog_id: str = None):
        """Запись метрик отправки сообщения"""
        self.messages_sent_total.labels(role=role).inc()
        
        if content:
            self.message_length_chars.observe(len(content))
        
        if role == 'user' and dialog_id:
            # Запоминаем время сообщения пользователя для вычисления времени ответа
            self.user_activity[dialog_id] = time.time()
        
        elif role == 'assistant' and dialog_id and dialog_id in self.user_activity:
            # Вычисляем время ответа
            response_time = time.time() - self.user_activity[dialog_id]
            self.assistant_response_time_seconds.observe(response_time)
            del self.user_activity[dialog_id]
    
    def record_redis_operation(self, operation: str, duration: float = 0):
        """Запись метрик Redis операций"""
        self.redis_operations_total.labels(operation=operation).inc()
        
        if duration > 0:
            self.redis_operation_duration_seconds.labels(operation=operation).observe(duration)
    
    def record_error(self, error_type: str, source: str):
        """Запись метрик ошибок"""
        self.errors_total.labels(type=error_type, source=source).inc()
    
    def record_dialog_completed(self, dialog_id: str, start_time: float, message_count: int):
        """Запись метрик завершения диалога"""
        duration = time.time() - start_time
        self.dialog_duration_seconds.observe(duration)
        
        if message_count > 0:
            self.messages_per_dialog.observe(message_count)
    
    def update_service_metrics(self):
        """Обновление системных метрик сервиса"""
        # Время работы сервиса
        uptime = time.time() - self.service_start_time
        self.service_uptime_seconds.set(uptime)
        
        # Использование памяти
        process = psutil.Process()
        memory_info = process.memory_info()
        self.memory_usage_bytes.set(memory_info.rss)
    
    def update_user_activity(self, user_id: str):
        """Обновление активности пользователя"""
        # В реальной реализации здесь была бы логика отслеживания активности
        pass
    
    def update_business_metrics(self, redis_client):
        """Обновление бизнес-метрик на основе данных Redis"""
        try:
            # Подсчет активных диалогов (обновленных за последние 24 часа)
            active_dialogs = 0
            cursor = 0
            
            while True:
                cursor, keys = redis_client.scan(cursor, match='dialog:*', count=100)
                
                for key in keys:
                    try:
                        updated_at = redis_client.hget(key, 'updated_at')
                        if updated_at:
                            # Парсим timestamp
                            dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                            if datetime.utcnow() - dt < timedelta(hours=24):
                                active_dialogs += 1
                    except:
                        continue
                
                if cursor == 0:
                    break
            
            self.active_dialogs.set(active_dialogs)
            
            # Подсчет активных пользователей
            active_users = len(redis_client.keys('user_dialogs:*'))
            self.active_users.set(active_users)
            
            logger.debug(f"Updated business metrics: active_dialogs={active_dialogs}, active_users={active_users}")
            
        except Exception as e:
            logger.error(f"Error updating business metrics: {e}")
            self.record_error('metrics_update', 'business_metrics')
    
    def get_metrics(self):
        """Получение всех метрик в формате Prometheus"""
        self.update_service_metrics()
        return generate_latest(self.registry)

# ============================================================================
# СЕРВИС ДИАЛОГОВ
# ============================================================================

class RedisDialogService:
    """Сервис для работы с диалогами в Redis"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", decode_responses: bool = True):
        self.redis_url = redis_url
        self.decode_responses = decode_responses
        self.redis = None
        self.scripts = {}
        self.metrics = DialogMetricsCollector()
        self.service_start_time = time.time()
    
    def connect(self):
        """Подключение к Redis и инициализация"""
        try:
            if self.redis_url.startswith("redis://"):
                import urllib.parse
                parsed = urllib.parse.urlparse(self.redis_url)
                host = parsed.hostname or "localhost"
                port = parsed.port or 6379
                password = parsed.password
                
                self.redis = redis.Redis(
                    host=host,
                    port=port,
                    password=password,
                    db=0,
                    decode_responses=self.decode_responses,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
            else:
                self.redis = redis.from_url(
                    self.redis_url,
                    decode_responses=self.decode_responses
                )
            
            # Проверяем подключение
            self.redis.ping()
            
            # Регистрируем Lua скрипты
            for name, script in LUA_SCRIPTS.items():
                self.scripts[name] = self.redis.register_script(script)
            
            logger.info(f"Connected to Redis at {self.redis_url}")
            logger.info(f"Registered {len(self.scripts)} Lua scripts")
            
            # Запускаем периодическое обновление метрик
            self.start_metrics_updater()
            
            return True
            
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def disconnect(self):
        """Отключение от Redis"""
        if self.redis:
            self.redis.close()
            logger.info("Disconnected from Redis")
    
    def start_metrics_updater(self):
        """Запуск периодического обновления метрик"""
        def update_metrics():
            while True:
                try:
                    self.metrics.update_business_metrics(self.redis)
                    time.sleep(60)  # Обновляем каждую минуту
                except Exception as e:
                    logger.error(f"Error in metrics updater: {e}")
                    time.sleep(10)
        
        thread = threading.Thread(target=update_metrics, daemon=True)
        thread.start()
        logger.info("Started metrics updater thread")
    
    # ============================================================================
    # ОСНОВНЫЕ МЕТОДЫ
    # ============================================================================
    
    def create_dialog(self, user_id: str, initial_message: Optional[Message] = None, 
                     metadata: Optional[Dict] = None) -> Tuple[str, Dict]:
        """Создание нового диалога"""
        start_time = time.time()
        
        try:
            # Генерация ID диалога
            dialog_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat()
            
            # Подготовка данных
            metadata = metadata or {}
            metadata_json = json.dumps(metadata)
            initial_message_json = json.dumps(initial_message.dict()) if initial_message else ''
            
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["create_dialog"](
                keys=[],
                args=[dialog_id, user_id, metadata_json, initial_message_json, timestamp]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'create_dialog')
                raise HTTPException(status_code=400, detail=str(result))
            
            # Запись метрик
            self.metrics.record_redis_operation('create_dialog', redis_duration)
            self.metrics.record_dialog_created(user_id, metadata)
            
            if initial_message:
                self.metrics.record_message_sent(
                    initial_message.role,
                    initial_message.content,
                    dialog_id
                )
            
            logger.info(f"Created dialog {dialog_id} for user {user_id}")
            
            return dialog_id, {
                'dialog_id': dialog_id,
                'user_id': user_id,
                'metadata': metadata,
                'created_at': timestamp,
                'updated_at': timestamp
            }
            
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'create_dialog')
            logger.error(f"Redis error creating dialog: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'create_dialog')
            logger.error(f"Error creating dialog: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Create dialog operation took {total_duration:.3f}s")
    
    def add_message(self, dialog_id: str, message: Message) -> Tuple[int, Dict]:
        """Добавление сообщения в диалог"""
        start_time = time.time()
        
        try:
            # Устанавливаем timestamp если не указан
            if not message.timestamp:
                message.timestamp = datetime.utcnow().isoformat()
            
            message_json = json.dumps(message.dict())
            timestamp = datetime.utcnow().isoformat()
            
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["add_message"](
                keys=[],
                args=[dialog_id, message_json, timestamp]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'add_message')
                raise HTTPException(status_code=404, detail=str(result))
            
            message_count = int(result)
            
            # Запись метрик
            self.metrics.record_redis_operation('add_message', redis_duration)
            self.metrics.record_message_sent(message.role, message.content, dialog_id)
            
            logger.info(f"Added message to dialog {dialog_id}, total: {message_count}")
            
            return message_count, {
                'dialog_id': dialog_id,
                'message_count': message_count,
                'message': message.dict(),
                'updated_at': timestamp
            }
            
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'add_message')
            logger.error(f"Redis error adding message: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'add_message')
            logger.error(f"Error adding message: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Add message operation took {total_duration:.3f}s")
    
    def get_dialog(self, dialog_id: str, start_idx: Optional[int] = None, 
                  end_idx: Optional[int] = None) -> Dict:
        """Получение диалога с сообщениями"""
        start_time = time.time()
        
        try:
            start_idx = start_idx or 0
            end_idx = end_idx or -1
            
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["get_dialog_with_messages"](
                keys=[],
                args=[dialog_id, start_idx, end_idx]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'get_dialog')
                raise HTTPException(status_code=404, detail=str(result))
            
            if not result or len(result) < 2:
                raise HTTPException(status_code=404, detail="Dialog not found")
            
            dialog_data, messages = result
            
            # Запись метрик
            self.metrics.record_redis_operation('get_dialog', redis_duration)
            
            response = {
                "dialog_id": dialog_id,
                "user_id": dialog_data.get('user_id', ''),
                "metadata": json.loads(dialog_data.get('metadata', '{}')),
                "created_at": dialog_data.get('created_at', ''),
                "updated_at": dialog_data.get('updated_at', ''),
                "messages": messages,
                "message_count": len(messages)
            }
            
            logger.info(f"Retrieved dialog {dialog_id} with {len(messages)} messages")
            
            return response
            
        except json.JSONDecodeError as e:
            self.metrics.record_error('json_error', 'get_dialog')
            logger.error(f"JSON decode error in dialog {dialog_id}: {e}")
            raise HTTPException(status_code=500, detail="Data corruption error")
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'get_dialog')
            logger.error(f"Redis error getting dialog: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'get_dialog')
            logger.error(f"Error getting dialog: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Get dialog operation took {total_duration:.3f}s")
    
    def get_user_dialogs(self, user_id: str, limit: int = 10, offset: int = 0) -> List[Dict]:
        """Получение списка диалогов пользователя"""
        start_time = time.time()
        
        try:
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["get_user_dialogs"](
                keys=[],
                args=[user_id, limit, offset]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'get_user_dialogs')
                raise HTTPException(status_code=404, detail=str(result))
            
            dialogs = json.loads(result) if result else []
            
            # Запись метрик
            self.metrics.record_redis_operation('get_user_dialogs', redis_duration)
            
            logger.info(f"Retrieved {len(dialogs)} dialogs for user {user_id}")
            
            return dialogs
            
        except json.JSONDecodeError as e:
            self.metrics.record_error('json_error', 'get_user_dialogs')
            logger.error(f"JSON decode error for user {user_id}: {e}")
            return []
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'get_user_dialogs')
            logger.error(f"Redis error getting user dialogs: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'get_user_dialogs')
            logger.error(f"Error getting user dialogs: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Get user dialogs operation took {total_duration:.3f}s")
    
    def delete_dialog(self, dialog_id: str) -> bool:
        """Удаление диалога"""
        start_time = time.time()
        
        try:
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["delete_dialog"](
                keys=[],
                args=[dialog_id]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'delete_dialog')
                raise HTTPException(status_code=404, detail=str(result))
            
            # Запись метрик
            self.metrics.record_redis_operation('delete_dialog', redis_duration)
            
            logger.info(f"Deleted dialog {dialog_id}")
            
            return result == 'deleted'
            
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'delete_dialog')
            logger.error(f"Redis error deleting dialog: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'delete_dialog')
            logger.error(f"Error deleting dialog: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Delete dialog operation took {total_duration:.3f}s")
    
    def search_messages(self, dialog_id: str, search_text: str, limit: int = 10) -> List[Dict]:
        """Поиск сообщений в диалоге"""
        start_time = time.time()
        
        try:
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["search_messages"](
                keys=[],
                args=[dialog_id, search_text, limit]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'search_messages')
                raise HTTPException(status_code=404, detail=str(result))
            
            results = json.loads(result) if result else []
            
            # Запись метрик
            self.metrics.record_redis_operation('search_messages', redis_duration)
            
            logger.info(f"Found {len(results)} messages in dialog {dialog_id} for search '{search_text}'")
            
            return results
            
        except json.JSONDecodeError as e:
            self.metrics.record_error('json_error', 'search_messages')
            logger.error(f"JSON decode error in search: {e}")
            return []
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'search_messages')
            logger.error(f"Redis error searching messages: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'search_messages')
            logger.error(f"Error searching messages: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Search messages operation took {total_duration:.3f}s")
    
    def get_dialog_stats(self, dialog_id: str) -> Dict:
        """Получение статистики диалога"""
        start_time = time.time()
        
        try:
            # Выполнение Lua скрипта
            script_start = time.time()
            result = self.scripts["get_dialog_stats"](
                keys=[],
                args=[dialog_id]
            )
            redis_duration = time.time() - script_start
            
            if isinstance(result, redis.ResponseError):
                self.metrics.record_error('redis_error', 'get_dialog_stats')
                raise HTTPException(status_code=404, detail=str(result))
            
            stats = json.loads(result) if result else {}
            
            # Запись метрик
            self.metrics.record_redis_operation('get_dialog_stats', redis_duration)
            
            logger.info(f"Retrieved stats for dialog {dialog_id}")
            
            return stats
            
        except json.JSONDecodeError as e:
            self.metrics.record_error('json_error', 'get_dialog_stats')
            logger.error(f"JSON decode error in stats: {e}")
            return {}
        except redis.RedisError as e:
            self.metrics.record_error('redis_error', 'get_dialog_stats')
            logger.error(f"Redis error getting dialog stats: {e}")
            raise HTTPException(status_code=500, detail=f"Redis error: {str(e)}")
        except Exception as e:
            self.metrics.record_error('unknown_error', 'get_dialog_stats')
            logger.error(f"Error getting dialog stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            total_duration = time.time() - start_time
            logger.debug(f"Get dialog stats operation took {total_duration:.3f}s")
    
    def complete_dialog(self, dialog_id: str, reason: str = "completed") -> bool:
        """Завершение диалога"""
        try:
            # Получаем информацию о диалоге для метрик
            dialog_info = self.get_dialog(dialog_id)
            start_time_str = dialog_info.get('created_at')
            message_count = dialog_info.get('message_count', 0)
            
            # Обновляем метаданные диалога
            dialog_key = f"dialog:{dialog_id}"
            current_metadata = self.redis.hget(dialog_key, 'metadata') or '{}'
            metadata = json.loads(current_metadata)
            
            metadata['completed'] = True
            metadata['completion_reason'] = reason
            metadata['completed_at'] = datetime.utcnow().isoformat()
            
            self.redis.hset(dialog_key, 'metadata', json.dumps(metadata))
            self.redis.hset(dialog_key, 'updated_at', datetime.utcnow().isoformat())
            
            # Запись метрик завершения
            if start_time_str:
                try:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00')).timestamp()
                    self.metrics.record_dialog_completed(dialog_id, start_time, message_count)
                except:
                    pass
            
            logger.info(f"Completed dialog {dialog_id} with reason: {reason}")
            
            return True
            
        except Exception as e:
            self.metrics.record_error('unknown_error', 'complete_dialog')
            logger.error(f"Error completing dialog: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def cleanup_old_dialogs(self, older_than_days: int = 30, limit: int = 100) -> int:
        """Очистка старых диалогов"""
        try:
            # Выполнение Lua скрипта
            result = self.scripts["cleanup_old_dialogs"](
                keys=[],
                args=[older_than_days, datetime.utcnow().isoformat(), limit]
            )
            
            deleted_count = int(result)
            logger.info(f"Cleaned up {deleted_count} old dialogs (older than {older_than_days} days)")
            
            return deleted_count
            
        except Exception as e:
            # logger.error(f"Error cleaning up old dialogs: {e}")
            return 0
    
    def get_service_metrics(self) -> bytes:
        """Получение метрик Prometheus"""
        return self.metrics.get_metrics()

# ============================================================================
# FASTAPI ПРИЛОЖЕНИЕ
# ============================================================================

# Создаем экземпляр сервиса
dialog_service = RedisDialogService(redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Запуск
    try:
        dialog_service.connect()
        logger.info("Dialog service with Prometheus metrics started successfully")
        
        # Запускаем периодическую очистку старых диалогов
        def cleanup_worker():
            while True:
                try:
                    deleted = dialog_service.cleanup_old_dialogs(older_than_days=90, limit=1000)
                    if deleted > 0:
                        logger.info(f"Cleanup worker deleted {deleted} old dialogs")
                except Exception as e:
                    logger.error(f"Error in cleanup worker: {e}")
                time.sleep(3600)  # Запускаем каждый час
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("Started cleanup worker thread")
        
    except Exception as e:
        logger.error(f"Failed to start dialog service: {e}")
        raise
    
    yield
    
    # Остановка
    dialog_service.disconnect()
    logger.info("Dialog service stopped")

# Создаем FastAPI приложение
app = FastAPI(
    title="Dialog Microservice with Prometheus",
    description="Микросервис для управления диалогами с полной поддержкой метрик Prometheus",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Middleware для сбора метрик API
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware для сбора метрик API запросов"""
    start_time = time.time()
    request_size = 0
    response_size = 0
    
    try:
        # Получаем размер запроса (если есть тело)
        if request.method in ["POST", "PUT", "PATCH"]:
            body = await request.body()
            request_size = len(body)
        
        # Обрабатываем запрос
        response = await call_next(request)
        
        # Получаем размер ответа
        if hasattr(response, 'body'):
            response_body = b''.join([chunk async for chunk in response.body_iterator])
            response_size = len(response_body)
            
            # Создаем новый response с тем же телом
            response = Response(
                content=response_body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type
            )
        
        # Записываем метрики
        duration = time.time() - start_time
        
        dialog_service.metrics.record_api_request(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code,
            duration=duration,
            request_size=request_size,
            response_size=response_size
        )
        
        return response
        
    except Exception as e:
        # Записываем метрики для ошибок
        duration = time.time() - start_time
        
        dialog_service.metrics.record_api_request(
            method=request.method,
            endpoint=request.url.path,
            status_code=500,
            duration=duration,
            request_size=request_size,
            response_size=response_size
        )
        
        dialog_service.metrics.record_error('http_exception', 'metrics_middleware')
        
        raise

# ============================================================================
# ЭНДПОИНТЫ API
# ============================================================================

@app.post("/dialogs", 
          response_model=Dict[str, str],
          summary="Создать новый диалог",
          description="Создает новый диалог для указанного пользователя")
async def create_dialog(request: CreateDialogRequest):
    """
    Создание нового диалога.
    
    - **user_id**: Идентификатор пользователя (обязательно)
    - **initial_message**: Первое сообщение в диалоге (опционально)
    - **metadata**: Метаданные диалога (источник, платформа и т.д.)
    """
    try:
        dialog_id, dialog_info = dialog_service.create_dialog(
            user_id=request.user_id,
            initial_message=request.initial_message,
            metadata=request.metadata
        )
        
        return {
            "dialog_id": dialog_id,
            "message": "Dialog created successfully",
            "created_at": dialog_info['created_at']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in create_dialog endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dialogs/{dialog_id}/messages",
          summary="Добавить сообщение в диалог",
          description="Добавляет новое сообщение в указанный диалог")
async def add_message(dialog_id: str, request: AddMessageRequest):
    """
    Добавление сообщения в диалог.
    
    - **dialog_id**: Идентификатор диалога (из URL)
    - **message**: Сообщение для добавления (обязательно)
    """
    try:
        message_count, result = dialog_service.add_message(dialog_id, request.message)
        
        return {
            "dialog_id": dialog_id,
            "message_count": message_count,
            "message": request.message.dict(),
            "updated_at": result['updated_at']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in add_message endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dialogs/{dialog_id}",
         response_model=Dialog,
         summary="Получить диалог",
         description="Возвращает диалог со всеми сообщениями")
async def get_dialog(dialog_id: str, start: Optional[int] = None, end: Optional[int] = None):
    """
    Получение диалога с сообщениями.
    
    - **dialog_id**: Идентификатор диалога (из URL)
    - **start**: Начальный индекс сообщений (опционально)
    - **end**: Конечный индекс сообщений (опционально)
    """
    try:
        dialog = dialog_service.get_dialog(dialog_id, start, end)
        return dialog
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_dialog endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/users/{user_id}/dialogs",
         summary="Получить диалоги пользователя",
         description="Возвращает список диалогов пользователя с пагинацией")
async def get_user_dialogs(user_id: str, limit: int = 10, offset: int = 0):
    """
    Получение диалогов пользователя.
    
    - **user_id**: Идентификатор пользователя (из URL)
    - **limit**: Количество диалогов на странице (по умолчанию 10)
    - **offset**: Смещение для пагинации (по умолчанию 0)
    """
    try:
        dialogs = dialog_service.get_user_dialogs(user_id, limit, offset)
        
        return {
            "user_id": user_id,
            "dialogs": dialogs,
            "total": len(dialogs),
            "limit": limit,
            "offset": offset
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_dialogs endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/dialogs/{dialog_id}",
            summary="Удалить диалог",
            description="Удаляет диалог и все его сообщения")
async def delete_dialog(dialog_id: str):
    """
    Удаление диалога.
    
    - **dialog_id**: Идентификатор диалога для удаления (из URL)
    """
    try:
        success = dialog_service.delete_dialog(dialog_id)
        
        return {
            "dialog_id": dialog_id,
            "deleted": success,
            "message": "Dialog deleted successfully" if success else "Failed to delete dialog"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in delete_dialog endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/dialogs/{dialog_id}/complete",
          summary="Завершить диалог",
          description="Отмечает диалог как завершенный")
async def complete_dialog(dialog_id: str, request: CompleteDialogRequest):
    """
    Завершение диалога.
    
    - **dialog_id**: Идентификатор диалога (из URL)
    - **completion_reason**: Причина завершения (по умолчанию "completed")
    """
    try:
        success = dialog_service.complete_dialog(dialog_id, request.completion_reason)
        
        return {
            "dialog_id": dialog_id,
            "completed": success,
            "completion_reason": request.completion_reason,
            "message": "Dialog marked as completed"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_dialog endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dialogs/{dialog_id}/search",
         summary="Поиск сообщений в диалоге",
         description="Ищет сообщения по тексту в указанном диалоге")
async def search_messages(dialog_id: str, q: str, limit: int = 10):
    """
    Поиск сообщений в диалоге.
    
    - **dialog_id**: Идентификатор диалога (из URL)
    - **q**: Текст для поиска (query parameter)
    - **limit**: Максимальное количество результатов (по умолчанию 10)
    """
    try:
        results = dialog_service.search_messages(dialog_id, q, limit)
        
        return {
            "dialog_id": dialog_id,
            "search_text": q,
            "results": results,
            "total_found": len(results)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in search_messages endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dialogs/{dialog_id}/stats",
         summary="Статистика диалога",
         description="Возвращает статистику по диалогу")
async def get_dialog_stats(dialog_id: str):
    """
    Получение статистики диалога.
    
    - **dialog_id**: Идентификатор диалога (из URL)
    """
    try:
        stats = dialog_service.get_dialog_stats(dialog_id)
        
        return {
            "dialog_id": dialog_id,
            "stats": stats,
            "retrieved_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_dialog_stats endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# ЭНДПОИНТЫ МОНИТОРИНГА И МЕТРИК
# ============================================================================

@app.get("/metrics",
         summary="Метрики Prometheus",
         description="Возвращает метрики в формате Prometheus",
         response_class=PlainTextResponse)
async def get_metrics():
    """
    Эндпоинт для сбора метрик Prometheus.
    
    Возвращает все метрики сервиса в текстовом формате Prometheus.
    """
    try:
        metrics_data = dialog_service.get_service_metrics()
        return Response(
            content=metrics_data,
            media_type=CONTENT_TYPE_LATEST
        )
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health",
         summary="Проверка здоровья",
         description="Проверяет состояние сервиса и зависимостей")
async def health_check():
    """
    Проверка здоровья сервиса.
    
    Проверяет подключение к Redis и общее состояние сервиса.
    """
    try:
        # Проверяем подключение к Redis
        redis_healthy = False
        if dialog_service.redis:
            dialog_service.redis.ping()
            redis_healthy = True
        
        # Собираем системные метрики
        system_metrics = {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage": psutil.disk_usage('/').percent if hasattr(psutil, 'disk_usage') else 0
        }
        
        # Время работы сервиса
        uptime = time.time() - dialog_service.service_start_time
        
        status = "healthy" if redis_healthy else "degraded"
        
        return {
            "status": status,
            "service": "dialog-microservice",
            "version": "2.0.0",
            "uptime_seconds": uptime,
            "redis": "connected" if redis_healthy else "disconnected",
            "metrics": "enabled",
            "system": system_metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "dialog-microservice",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

@app.get("/stats",
         summary="Статистика сервиса",
         description="Возвращает расширенную статистику сервиса")
async def get_service_stats():
    """
    Получение расширенной статистики сервиса.
    
    Включает информацию о Redis, метрики и системную информацию.
    """
    try:
        stats = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": "dialog-microservice",
            "version": "2.0.0",
            "uptime_seconds": time.time() - dialog_service.service_start_time
        }
        
        if dialog_service.redis:
            try:
                info = dialog_service.redis.info()
                
                stats["redis"] = {
                    "version": info.get("redis_version", "unknown"),
                    "mode": info.get("redis_mode", "standalone"),
                    "os": info.get("os", "unknown"),
                    "used_memory": info.get("used_memory_human", "0"),
                    "used_memory_peak": info.get("used_memory_peak_human", "0"),
                    "connected_clients": info.get("connected_clients", 0),
                    "total_connections_received": info.get("total_connections_received", 0),
                    "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                    "total_keys": dialog_service.redis.dbsize(),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0)
                }
                
                # Вычисляем hit rate
                hits = info.get("keyspace_hits", 0)
                misses = info.get("keyspace_misses", 0)
                total = hits + misses
                stats["redis"]["hit_rate_percent"] = (hits / total * 100) if total > 0 else 0
                
            except Exception as e:
                stats["redis_error"] = str(e)
        
        # Системная информация
        stats["system"] = {
            "cpu_count": psutil.cpu_count(),
            "cpu_percent": psutil.cpu_percent(),
            "memory_total_gb": psutil.virtual_memory().total / (1024**3),
            "memory_available_gb": psutil.virtual_memory().available / (1024**3),
            "memory_percent": psutil.virtual_memory().percent
        }
        
        # Информация о процессе
        process = psutil.Process()
        stats["process"] = {
            "pid": process.pid,
            "create_time": process.create_time(),
            "cpu_percent": process.cpu_percent(),
            "memory_rss_mb": process.memory_info().rss / (1024**2),
            "memory_vms_mb": process.memory_info().vms / (1024**2),
            "num_threads": process.num_threads(),
            "num_fds": process.num_fds() if hasattr(process, 'num_fds') else 0
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting service stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ready",
         summary="Проверка готовности",
         description="Проверяет готовность сервиса к обработке запросов")
async def readiness_check():
    """
    Проверка готовности сервиса.
    
    Используется для проверки readiness в Kubernetes/Orchestration.
    """
    try:
        # Проверяем подключение к Redis
        if not dialog_service.redis:
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "reason": "Redis not connected"}
            )
        
        dialog_service.redis.ping()
        
        return {"status": "ready", "timestamp": datetime.utcnow().isoformat()}
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": str(e)}
        )

@app.get("/",
         summary="Информация о сервисе",
         description="Возвращает основную информацию о сервисе")
async def root():
    """
    Корневой эндпоинт сервиса.
    
    Возвращает информацию о сервисе и доступные эндпоинты.
    """
    return {
        "service": "Dialog Microservice",
        "version": "2.0.0",
        "description": "Микросервис для управления диалогами с поддержкой Prometheus метрик",
        "endpoints": {
            "docs": "/docs",
            "redoc": "/redoc",
            "health": "/health",
            "metrics": "/metrics",
            "stats": "/stats",
            "create_dialog": "POST /dialogs",
            "add_message": "POST /dialogs/{dialog_id}/messages",
            "get_dialog": "GET /dialogs/{dialog_id}",
            "get_user_dialogs": "GET /users/{user_id}/dialogs",
            "delete_dialog": "DELETE /dialogs/{dialog_id}",
            "complete_dialog": "POST /dialogs/{dialog_id}/complete",
            "search_messages": "GET /dialogs/{dialog_id}/search",
            "dialog_stats": "GET /dialogs/{dialog_id}/stats"
        },
        "features": [
            "Управление диалогами",
            "Хранение сообщений",
            "Поиск по сообщениям",
            "Метрики Prometheus",
            "Мониторинг здоровья",
            "Автоочистка старых данных"
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

# ============================================================================
# ЗАПУСК СЕРВИСА
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    import argparse
    
    parser = argparse.ArgumentParser(description="Dialog Microservice with Prometheus")
    parser.add_argument("--host", default=os.environ.get("APP_HOST", "localhost"), help="Host to bind")
    parser.add_argument("--port", type=int, default=int(os.environ.get("APP_PORT", "8000")), help="Port to bind")
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379"), help="Redis URL")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--log-level", default="info", help="Log level")

    args = parser.parse_args()
    
    # Обновляем URL Redis если передан
    if args.redis_url != "redis://localhost:6379":
        dialog_service.redis_url = args.redis_url
    
    uvicorn.run(
        "main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
        access_log=True
    )
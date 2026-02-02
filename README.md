# 🗣️ Dialog Service - Микросервис управления диалогами

## 📋 Краткое описание

**Dialog Service** — это высокопроизводительный микросервис на FastAPI для управления диалогами пользователей с поддержкой Redis в качестве основного хранилища и Lua-скриптов для атомарных операций.

---

## 🎯 Основные возможности

### 💬 **Управление диалогами**
- Создание новых диалогов
- Добавление сообщений (user/assistant/system)
- Получение диалогов с пагинацией
- Поиск по сообщениям
- Удаление и архивация диалогов

### 🚀 **Производительность**
- **Lua-скрипты в Redis** для атомарных операций
- **Асинхронная обработка** на FastAPI
- **Эффективное хранение** данных в Redis
- **Пакетные операции** для массовых действий

### 📊 **Мониторинг и метрики**
- **Prometheus метрики** для всех операций
- **Бизнес-метрики**: активные пользователи, retention rate
- **API метрики**: response time, error rate, throughput
- **Redis метрики**: операции, latency, hit rate

### 🔧 **Технические особенности**
- **REST API** с OpenAPI документацией
- **Переменные окружения** для конфигурации
- **Docker контейнеризация**
- **Health checks** и readiness probes
- **Автоматическая очистка** старых диалогов

---

## 🏗️ Архитектура

```
┌─────────────────┐    HTTP/REST    ┌─────────────────┐
│   Клиенты       │◄───────────────►│   Dialog Service│
│   (Web/Mobile)  │                 │   (FastAPI)     │
└─────────────────┘                 └─────────┬───────┘
                                              │
                                              │ Prometheus
                                              │ Metrics
                                              │
                                       ┌──────▼──────┐
                                       │   Redis     │
                                       │   (Storage) │
                                       └─────────────┘
```

---

## 📊 Ключевые метрики

| Категория | Метрики | Назначение |
|-----------|---------|------------|
| **API** | `dialog_api_requests_total`, `dialog_api_request_duration_seconds` | Мониторинг производительности API |
| **Бизнес** | `dialog_active_users`, `dialog_user_retention_rate` | Анализ пользовательской активности |
| **Redis** | `dialog_redis_operations_total`, `dialog_redis_operation_duration_seconds` | Мониторинг хранилища |
| **Система** | `dialog_service_uptime_seconds`, `dialog_memory_usage_bytes` | Мониторинг инфраструктуры |

---

## 🛠️ Технологический стек

- **Backend**: Python 3.11+, FastAPI, Pydantic
- **База данных**: Redis 7+ с Lua-скриптами
- **Мониторинг**: Prometheus, Grafana
- **Контейнеризация**: Docker, Docker Compose
- **Метрики**: Prometheus Client
- **Логирование**: Structured logging

---

## 🚀 Быстрый старт

### 1. Запуск с Docker Compose
```bash
docker-compose up -d
```

### 2. Проверка работы
```bash
# Health check
curl http://localhost:8000/health

# Создание диалога
curl -X POST http://localhost:8000/dialogs \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user_123", "initial_message": {"role": "user", "content": "Привет!"}}'
```

### 3. Доступ к интерфейсам
- **API**: http://localhost:8000/docs
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090

#!/usr/bin/env python3
"""
Исправленный скрипт для мониторинга Redis
"""

import asyncio
import redis
import time
from datetime import datetime
import json
import psutil
import argparse
import logging
from typing import Dict, Any, List
import threading
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisMonitor:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis = None
        self.running = False
        self.lock = threading.Lock()

    def connect(self):
        """Подключение к Redis"""
        try:
            self.redis = redis.Redis.from_url(self.redis_url, decode_responses=True)
            # Проверяем подключение
            self.redis.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def disconnect(self):
        """Отключение от Redis"""
        if self.redis:
            self.redis.close()
            logger.info("Disconnected from Redis")

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики Redis"""
        try:
            with self.lock:
                if not self.redis:
                    return {}

                # Команда INFO возвращает всю статистику
                info = self.redis.info()

                # Получаем количество ключей
                db_size = self.redis.dbsize()

                # Получаем статистику по памяти
                memory_info = self.redis.info('memory')

                # Получаем список всех ключей (осторожно - может быть много!)
                # Используем SCAN для безопасного подсчета
                dialog_keys = self.redis.scan(match='dialog:*', count=1000)[1]
                message_keys = self.redis.scan(match='dialog_messages:*', count=1000)[1]
                user_keys = self.redis.scan(match='user_dialogs:*', count=1000)[1]

                # Подсчитываем общее количество сообщений
                total_messages = 0
                for key in message_keys:
                    try:
                        total_messages += self.redis.llen(key)
                    except:
                        pass

                # Получаем клиентов
                client_list = self.redis.client_list()

                # Получаем статистику команд
                command_stats = self.redis.info('commandstats')

                return {
                    "timestamp": datetime.now().isoformat(),
                    "redis": {
                        "connected_clients": len(client_list),
                        "used_memory": memory_info.get('used_memory_human', '0'),
                        "total_connections_received": info.get('total_connections_received', 0),
                        "total_commands_processed": info.get('total_commands_processed', 0),
                        "instantaneous_ops_per_sec": info.get('instantaneous_ops_per_sec', 0),
                        "keyspace_hits": info.get('keyspace_hits', 0),
                        "keyspace_misses": info.get('keyspace_misses', 0),
                        "hit_rate": info.get('keyspace_hits', 0) / max(
                            info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0), 1) * 100,
                        "total_keys": db_size,
                        "memory_fragmentation_ratio": memory_info.get('mem_fragmentation_ratio', 0),
                        "used_memory_peak": memory_info.get('used_memory_peak_human', '0')
                    },
                    "application": {
                        "total_dialogs": len(dialog_keys),
                        "total_messages": total_messages,
                        "total_users": len(user_keys),
                        "avg_messages_per_dialog": total_messages / max(len(dialog_keys), 1)
                    },
                    "system": {
                        "cpu_percent": psutil.cpu_percent(),
                        "memory_percent": psutil.virtual_memory().percent,
                        "disk_usage": psutil.disk_usage('/').percent
                    },
                    "commands": {
                        k.split('_')[1]: v['calls'] for k, v in command_stats.items() if 'calls' in v
                    }
                }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }

    def get_slow_log(self, count: int = 10) -> List[Dict]:
        """Получение медленных запросов"""
        try:
            with self.lock:
                if not self.redis:
                    return []

                slow_log = self.redis.slowlog_get(count)
                return [
                    {
                        "id": entry['id'],
                        "timestamp": datetime.fromtimestamp(entry['start_time']).isoformat(),
                        "duration_ms": entry['duration'] / 1000,
                        "command": ' '.join(
                            entry['command'].decode('utf-8', errors='ignore').split()[:5]) if isinstance(
                            entry['command'], bytes) else ' '.join(str(entry['command']).split()[:5])
                    }
                    for entry in slow_log
                ]
        except Exception as e:
            logger.error(f"Error getting slow log: {e}")
            return []

    def get_memory_usage_by_pattern(self, pattern: str = "dialog:*") -> Dict:
        """Получение использования памяти по шаблону"""
        try:
            with self.lock:
                if not self.redis:
                    return {}

                cursor = 0
                total_size = 0
                key_count = 0

                while True:
                    cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                    if not keys:
                        break

                    for key in keys:
                        try:
                            # Получаем тип ключа и размер
                            key_type = self.redis.type(key)
                            if key_type == 'string':
                                size = self.redis.memory_usage(key)
                            elif key_type == 'hash':
                                size = sum(self.redis.hlen(key) * 100 for _ in range(10))  # Примерная оценка
                            elif key_type == 'list':
                                size = self.redis.llen(key) * 100  # Примерная оценка
                            else:
                                size = 0

                            if size:
                                total_size += size
                                key_count += 1
                        except:
                            pass

                    if cursor == 0:
                        break

                return {
                    "pattern": pattern,
                    "key_count": key_count,
                    "total_size_bytes": total_size,
                    "total_size_mb": total_size / (1024 * 1024)
                }
        except Exception as e:
            logger.error(f"Error getting memory usage: {e}")
            return {}

    def monitor_continuously(self, interval: int = 5, duration: int = 300):
        """Непрерывный мониторинг"""
        if not self.connect():
            logger.error("Cannot connect to Redis. Exiting.")
            return

        end_time = time.time() + duration
        stats_history = []

        print(f"\n{'=' * 80}")
        print("МОНИТОРИНГ REDIS И СИСТЕМЫ")
        print(f"Интервал: {interval} сек, Продолжительность: {duration} сек")
        print(f"{'=' * 80}\n")

        try:
            while time.time() < end_time:
                stats = self.get_stats()

                if "error" in stats:
                    print(f"[{stats['timestamp']}] Ошибка: {stats['error']}")
                else:
                    # Выводим текущую статистику
                    print(f"\n[{stats['timestamp']}]")
                    print(f"Redis: {stats['redis']['connected_clients']} клиентов, "
                          f"{stats['redis']['instantaneous_ops_per_sec']} оп/сек, "
                          f"Память: {stats['redis']['used_memory']}, "
                          f"Hit Rate: {stats['redis']['hit_rate']:.1f}%")

                    print(f"Приложение: {stats['application']['total_dialogs']} диалогов, "
                          f"{stats['application']['total_messages']} сообщений, "
                          f"{stats['application']['total_users']} пользователей")

                    print(f"Система: CPU {stats['system']['cpu_percent']:5.1f}%, "
                          f"RAM {stats['system']['memory_percent']:5.1f}%, "
                          f"Disk {stats['system']['disk_usage']:5.1f}%")

                    # Выводим топ команд
                    if stats.get('commands'):
                        top_commands = sorted(stats['commands'].items(),
                                              key=lambda x: x[1], reverse=True)[:3]
                        if top_commands:
                            print(f"Топ команд: {', '.join(f'{k}:{v}' for k, v in top_commands)}")

                # Проверяем медленные запросы
                slow_log = self.get_slow_log(2)
                if slow_log:
                    print(f"Медленные запросы:")
                    for entry in slow_log:
                        print(f"  {entry['duration_ms']:6.1f} мс: {entry['command']}")

                # Периодически проверяем использование памяти по шаблонам
                if len(stats_history) % 10 == 0:
                    memory_stats = self.get_memory_usage_by_pattern("dialog:*")
                    if memory_stats.get('key_count', 0) > 0:
                        print(f"Память (dialog:*): {memory_stats['key_count']} ключей, "
                              f"{memory_stats['total_size_mb']:.2f} MB")

                stats_history.append(stats)
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\n\nМониторинг остановлен пользователем")
        except Exception as e:
            logger.error(f"Error during monitoring: {e}")
        finally:
            self.disconnect()

        # Сохраняем историю
        if stats_history:
            self.save_history(stats_history)
            self.analyze_stats(stats_history)

    def save_history(self, stats_history: List[Dict]):
        """Сохранение истории мониторинга"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"monitoring_history_{timestamp}.json"

        try:
            # Фильтруем только успешные записи
            valid_stats = [s for s in stats_history if "error" not in s]

            with open(filename, "w") as f:
                json.dump(valid_stats, f, indent=2, default=str)

            logger.info(f"История мониторинга сохранена в {filename}")

            # Также сохраняем сводный отчет
            self.save_summary_report(valid_stats, timestamp)

        except Exception as e:
            logger.error(f"Error saving history: {e}")

    def save_summary_report(self, stats_history: List[Dict], timestamp: str):
        """Сохранение сводного отчета"""
        if not stats_history:
            return

        report = {
            "monitoring_start": stats_history[0].get('timestamp'),
            "monitoring_end": stats_history[-1].get('timestamp'),
            "total_samples": len(stats_history),
            "summary": {}
        }

        # Извлекаем метрики
        metrics_to_collect = [
            ('redis.instantaneous_ops_per_sec', 'ops_per_sec'),
            ('redis.connected_clients', 'connected_clients'),
            ('redis.hit_rate', 'hit_rate'),
            ('application.total_dialogs', 'total_dialogs'),
            ('application.total_messages', 'total_messages'),
            ('system.cpu_percent', 'cpu_percent'),
            ('system.memory_percent', 'memory_percent')
        ]

        for metric_path, metric_name in metrics_to_collect:
            values = []
            for stat in stats_history:
                # Извлекаем значение по пути
                parts = metric_path.split('.')
                value = stat
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        value = None
                        break

                if value is not None and isinstance(value, (int, float)):
                    values.append(value)

            if values:
                report['summary'][metric_name] = {
                    'avg': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values),
                    'samples': len(values)
                }

        # Сохраняем отчет
        report_filename = f"monitoring_report_{timestamp}.json"
        with open(report_filename, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Сводный отчет сохранен в {report_filename}")

        # Выводим основные выводы
        self.print_report_summary(report)

    def print_report_summary(self, report: Dict):
        """Печать сводки отчета"""
        print(f"\n{'=' * 80}")
        print("СВОДНЫЙ ОТЧЕТ МОНИТОРИНГА")
        print(f"{'=' * 80}")

        if 'summary' in report:
            summary = report['summary']

            if 'ops_per_sec' in summary:
                ops = summary['ops_per_sec']
                print(f"\nНагрузка Redis:")
                print(f"  Средняя: {ops['avg']:.1f} оп/сек")
                print(f"  Минимальная: {ops['min']:.1f} оп/сек")
                print(f"  Максимальная: {ops['max']:.1f} оп/сек")

            if 'hit_rate' in summary:
                hr = summary['hit_rate']
                print(f"\nЭффективность кэша:")
                print(f"  Средний hit rate: {hr['avg']:.1f}%")
                status = "ХОРОШО" if hr['avg'] > 80 else "ПЛОХО" if hr['avg'] < 50 else "НОРМА"
                print(f"  Статус: {status}")

            if 'cpu_percent' in summary:
                cpu = summary['cpu_percent']
                print(f"\nИспользование CPU:")
                print(f"  Среднее: {cpu['avg']:.1f}%")
                print(f"  Пиковое: {cpu['max']:.1f}%")

            if 'total_dialogs' in summary:
                dialogs = summary['total_dialogs']
                print(f"\nДанные приложения:")
                print(f"  Диалогов: {dialogs['avg']:.0f} (в среднем)")

            # Проверяем пороговые значения
            warnings = []
            if 'ops_per_sec' in summary and summary['ops_per_sec']['max'] > 5000:
                warnings.append("Очень высокая нагрузка на Redis (>5000 оп/сек)")
            if 'hit_rate' in summary and summary['hit_rate']['avg'] < 70:
                warnings.append("Низкий hit rate (<70%) - возможна нехватка памяти")
            if 'cpu_percent' in summary and summary['cpu_percent']['avg'] > 80:
                warnings.append("Высокая загрузка CPU (>80%)")

            if warnings:
                print(f"\n{'!' * 40}")
                print("ВНИМАНИЕ - обнаружены проблемы:")
                for warning in warnings:
                    print(f"  • {warning}")
                print(f"{'!' * 40}")

    def analyze_stats(self, stats_history: List[Dict]):
        """Анализ собранной статистики"""
        print(f"\n{'=' * 80}")
        print("АНАЛИЗ СТАТИСТИКИ")
        print(f"{'=' * 80}")

        # Фильтруем только успешные записи
        valid_stats = [s for s in stats_history if "error" not in s]

        if not valid_stats:
            print("Нет данных для анализа")
            return

        # Извлекаем метрики
        ops_per_sec = []
        connected_clients = []
        hit_rates = []
        dialogs_count = []

        for stat in valid_stats:
            redis_info = stat.get('redis', {})
            app_info = stat.get('application', {})

            if 'instantaneous_ops_per_sec' in redis_info:
                ops_per_sec.append(redis_info['instantaneous_ops_per_sec'])
            if 'connected_clients' in redis_info:
                connected_clients.append(redis_info['connected_clients'])
            if 'hit_rate' in redis_info:
                hit_rates.append(redis_info['hit_rate'])
            if 'total_dialogs' in app_info:
                dialogs_count.append(app_info['total_dialogs'])

        if ops_per_sec:
            print(f"Нагрузка Redis:")
            print(f"  Средняя: {sum(ops_per_sec) / len(ops_per_sec):.1f} оп/сек")
            print(f"  Пиковая: {max(ops_per_sec):.1f} оп/сек")
            print(f"  Минимальная: {min(ops_per_sec):.1f} оп/сек")

        if hit_rates:
            avg_hit_rate = sum(hit_rates) / len(hit_rates)
            print(f"\nЭффективность кэша:")
            print(f"  Средний hit rate: {avg_hit_rate:.1f}%")
            if avg_hit_rate > 90:
                print("  Отлично! Кэш работает эффективно")
            elif avg_hit_rate > 70:
                print("  Хорошо. Кэш работает нормально")
            elif avg_hit_rate > 50:
                print("  Удовлетворительно. Возможны оптимизации")
            else:
                print("  Плохо! Нужно увеличить память Redis или оптимизировать запросы")

        if dialogs_count and len(set(dialogs_count)) > 1:
            print(f"\nИзменение данных:")
            print(f"  Начало: {dialogs_count[0]:.0f} диалогов")
            print(f"  Конец: {dialogs_count[-1]:.0f} диалогов")
            if dialogs_count[-1] > dialogs_count[0]:
                growth = (dialogs_count[-1] - dialogs_count[0]) / max(dialogs_count[0], 1) * 100
                print(f"  Рост: +{growth:.1f}%")


def main():
    parser = argparse.ArgumentParser(description='Redis Monitor')
    parser.add_argument('--redis-url', type=str, default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                        help='URL Redis (по умолчанию: redis://localhost:6379)')
    parser.add_argument('--interval', type=int, default=5,
                        help='Интервал обновления в секундах (по умолчанию: 5)')
    parser.add_argument('--duration', type=int, default=300,
                        help='Продолжительность мониторинга в секундах (по умолчанию: 300)')
    parser.add_argument('--pattern', type=str, default='dialog:*',
                        help='Шаблон ключей для анализа памяти')

    args = parser.parse_args()

    monitor = RedisMonitor(args.redis_url)
    monitor.monitor_continuously(args.interval, args.duration)


if __name__ == "__main__":
    main()
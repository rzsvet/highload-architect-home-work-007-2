#!/usr/bin/env python3
"""
Исправленный скрипт для нагрузочного тестирования
"""

import asyncio
import aiohttp
import redis
import random
import time
import uuid
import json
from datetime import datetime
from typing import List, Dict, Any, Tuple
import argparse
import statistics
from dataclasses import dataclass
import logging
from faker import Faker
import os

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Инициализация Faker для генерации реалистичных данных
fake = Faker('ru_RU')  # Русские данные


@dataclass
class TestResult:
    operation: str
    success_count: int
    failure_count: int
    total_time: float
    avg_response_time: float
    min_response_time: float
    max_response_time: float
    p95_response_time: float
    p99_response_time: float


class DialogLoadTester:
    def __init__(self, base_url: str = "http://localhost:8000", redis_url: str = "redis://localhost:6379"):
        self.base_url = base_url.rstrip('/')
        self.redis_url = redis_url
        self.redis = None
        self.session = None
        self.created_dialogs = []
        self.created_users = set()

    async def init(self):
        """Инициализация подключений"""
        self.session = aiohttp.ClientSession()
        self.redis = redis.Redis.from_url(self.redis_url, decode_responses=True)
        logger.info("Подключения инициализированы")

    async def close(self):
        """Закрытие подключений"""
        if self.session:
            await self.session.close()
        if self.redis:
            self.redis.close()
        logger.info("Подключения закрыты")

    def generate_user_id(self) -> str:
        """Генерация ID пользователя"""
        user_id = f"user_{random.randint(1000, 9999)}_{int(time.time())}"
        self.created_users.add(user_id)
        return user_id

    def generate_message(self, role: str = None) -> Dict[str, Any]:
        """Генерация случайного сообщения"""
        if role is None:
            role = random.choice(["user", "assistant"])

        # Генерация реалистичного контента
        if role == "user":
            content = random.choice([
                f"Привет! {fake.sentence()}",
                f"У меня вопрос: {fake.sentence()}",
                f"Помогите с {fake.word()}: {fake.sentence()}",
                f"Как сделать {fake.word()}? {fake.sentence()}",
                f"Скажите, {fake.sentence()}",
                f"{fake.sentence()} Что вы думаете?",
                f"Мне нужно {fake.word()}. {fake.sentence()}",
                f"{fake.sentence()} Можете помочь?",
                f"Расскажите про {fake.word()}. {fake.sentence()}",
                f"Почему {fake.sentence()}"
            ])
        else:
            content = random.choice([
                f"Здравствуйте! {fake.sentence()}",
                f"Конечно! {fake.sentence()}",
                f"Я могу помочь. {fake.sentence()}",
                f"Понимаю ваш вопрос. {fake.sentence()}",
                f"Спасибо за обращение! {fake.sentence()}",
                f"Отличный вопрос! {fake.sentence()}",
                f"Давайте разберемся. {fake.sentence()}",
                f"Вот что я могу предложить: {fake.sentence()}",
                f"Интересный вопрос. {fake.sentence()}",
                f"Мой ответ: {fake.sentence()}"
            ])

        return {
            "role": role,
            "content": content,
            "timestamp": datetime.utcnow().isoformat()
        }

    def generate_metadata(self) -> Dict[str, Any]:
        """Генерация случайных метаданных"""
        sources = ["web", "mobile", "api", "telegram", "whatsapp", "viber"]
        categories = ["support", "sales", "technical", "general", "feedback", "complaint"]

        return {
            "source": random.choice(sources),
            "category": random.choice(categories),
            "language": "ru",
            "platform": random.choice(["web", "android", "ios", "desktop"]),
            "user_agent": fake.user_agent(),
            "ip_address": fake.ipv4(),
            "created_via": "load_test"
        }

    async def create_dialog(self, user_id: str = None) -> Tuple[bool, str, float]:
        """Создание одного диалога"""
        if user_id is None:
            user_id = self.generate_user_id()

        start_time = time.time()

        try:
            payload = {
                "user_id": user_id,
                "initial_message": self.generate_message("user"),
                "metadata": self.generate_metadata()
            }

            async with self.session.post(
                    f"{self.base_url}/dialogs",
                    json=payload,
                    timeout=10
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    dialog_id = data.get("dialog_id")
                    if dialog_id:
                        self.created_dialogs.append(dialog_id)
                        return True, dialog_id, time.time() - start_time
                    else:
                        logger.error(f"No dialog_id in response: {data}")
                        return False, None, time.time() - start_time
                else:
                    logger.error(f"Failed to create dialog: {response.status}")
                    return False, None, time.time() - start_time

        except Exception as e:
            logger.error(f"Error creating dialog: {e}")
            return False, None, time.time() - start_time

    async def add_message_to_dialog(self, dialog_id: str) -> Tuple[bool, float]:
        """Добавление сообщения в диалог"""
        start_time = time.time()

        try:
            payload = {
                "message": self.generate_message(random.choice(["user", "assistant"]))
            }

            async with self.session.post(
                    f"{self.base_url}/dialogs/{dialog_id}/messages",
                    json=payload,
                    timeout=10
            ) as response:
                if response.status == 200:
                    return True, time.time() - start_time
                else:
                    logger.error(f"Failed to add message: {response.status}")
                    return False, time.time() - start_time

        except Exception as e:
            logger.error(f"Error adding message: {e}")
            return False, time.time() - start_time

    async def get_dialog(self, dialog_id: str) -> Tuple[bool, float]:
        """Получение диалога"""
        start_time = time.time()

        try:
            async with self.session.get(
                    f"{self.base_url}/dialogs/{dialog_id}",
                    timeout=10
            ) as response:
                if response.status == 200:
                    return True, time.time() - start_time
                else:
                    logger.error(f"Failed to get dialog: {response.status}")
                    return False, time.time() - start_time

        except Exception as e:
            logger.error(f"Error getting dialog: {e}")
            return False, time.time() - start_time

    async def get_user_dialogs(self, user_id: str) -> Tuple[bool, float]:
        """Получение диалогов пользователя"""
        start_time = time.time()

        try:
            async with self.session.get(
                    f"{self.base_url}/users/{user_id}/dialogs",
                    params={"limit": 10, "offset": 0},
                    timeout=10
            ) as response:
                if response.status == 200:
                    return True, time.time() - start_time
                else:
                    logger.error(f"Failed to get user dialogs: {response.status}")
                    return False, time.time() - start_time

        except Exception as e:
            logger.error(f"Error getting user dialogs: {e}")
            return False, time.time() - start_time

    async def delete_dialog(self, dialog_id: str) -> Tuple[bool, float]:
        """Удаление диалога"""
        start_time = time.time()

        try:
            async with self.session.delete(
                    f"{self.base_url}/dialogs/{dialog_id}",
                    timeout=10
            ) as response:
                if response.status == 200:
                    return True, time.time() - start_time
                else:
                    logger.error(f"Failed to delete dialog: {response.status}")
                    return False, time.time() - start_time

        except Exception as e:
            logger.error(f"Error deleting dialog: {e}")
            return False, time.time() - start_time

    async def run_concurrent_operations(self, operation_func, items, concurrency: int) -> TestResult:
        """Выполнение операций с заданной конкурентностью"""
        semaphore = asyncio.Semaphore(concurrency)
        response_times = []
        success_count = 0
        failure_count = 0

        async def run_operation(item):
            async with semaphore:
                success, response_time = await operation_func(item)
                response_times.append(response_time)
                return success

        # Создаем задачи
        tasks = [run_operation(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Обрабатываем результаты
        for result in results:
            if isinstance(result, Exception):
                failure_count += 1
                logger.error(f"Task failed with exception: {result}")
            elif result:
                success_count += 1
            else:
                failure_count += 1

        # Вычисляем статистику
        if response_times:
            avg_time = statistics.mean(response_times)
            min_time = min(response_times)
            max_time = max(response_times)
            p95_time = statistics.quantiles(response_times, n=100)[94] if len(response_times) > 1 else avg_time
            p99_time = statistics.quantiles(response_times, n=100)[98] if len(response_times) > 1 else avg_time
        else:
            avg_time = min_time = max_time = p95_time = p99_time = 0

        return TestResult(
            operation=operation_func.__name__,
            success_count=success_count,
            failure_count=failure_count,
            total_time=sum(response_times),
            avg_response_time=avg_time,
            min_response_time=min_time,
            max_response_time=max_time,
            p95_response_time=p95_time,
            p99_response_time=p99_time
        )

    async def populate_database(self, num_dialogs: int, messages_per_dialog: int, concurrency: int = 10):
        """Наполнение базы данных случайными данными"""
        logger.info(f"Начинаем наполнение базы: {num_dialogs} диалогов, {messages_per_dialog} сообщений на диалог")

        start_time = time.time()

        # Создаем диалоги
        logger.info("Создаем диалоги...")
        user_ids = [self.generate_user_id() for _ in range(num_dialogs)]

        create_results = []
        for i in range(0, num_dialogs, concurrency):
            batch = user_ids[i:i + concurrency]
            tasks = [self.create_dialog(user_id) for user_id in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            create_results.extend(batch_results)

            if (i + len(batch)) % 100 == 0:
                logger.info(f"Создано {i + len(batch)}/{num_dialogs} диалогов")

        # Анализ результатов создания диалогов
        successful_dialogs = []
        for result in create_results:
            if isinstance(result, tuple) and result[0]:
                successful_dialogs.append(result[1])

        logger.info(f"Успешно создано {len(successful_dialogs)}/{num_dialogs} диалогов")

        # Добавляем сообщения в диалоги
        logger.info("Добавляем сообщения в диалоги...")
        all_messages_tasks = []

        for dialog_id in successful_dialogs:
            for _ in range(messages_per_dialog):
                all_messages_tasks.append(self.add_message_to_dialog(dialog_id))

        # Выполняем добавление сообщений пакетами
        message_results = []
        for i in range(0, len(all_messages_tasks), concurrency * 10):
            batch = all_messages_tasks[i:i + concurrency * 10]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            message_results.extend(batch_results)

            if i > 0 and i % (concurrency * 100) == 0:
                logger.info(f"Добавлено {i}/{len(all_messages_tasks)} сообщений")

        # Анализ результатов добавления сообщений
        successful_messages = sum(1 for r in message_results if isinstance(r, tuple) and r[0])

        total_time = time.time() - start_time

        logger.info(f"Наполнение базы завершено за {total_time:.2f} секунд")
        logger.info(f"Итог: {len(successful_dialogs)} диалогов, {successful_messages} сообщений")

        return {
            "total_dialogs_attempted": num_dialogs,
            "successful_dialogs": len(successful_dialogs),
            "total_messages_attempted": num_dialogs * messages_per_dialog,
            "successful_messages": successful_messages,
            "total_time": total_time,
            "dialogs_per_second": len(successful_dialogs) / total_time if total_time > 0 else 0,
            "messages_per_second": successful_messages / total_time if total_time > 0 else 0
        }

    async def run_load_test(self,
                            num_users: int,
                            operations_per_user: int,
                            operation_mix: Dict[str, float],
                            concurrency: int = 20,
                            duration: int = 60):
        """Запуск нагрузочного теста"""
        logger.info(
            f"Начинаем нагрузочный тест: {num_users} пользователей, {operations_per_user} операций на пользователя")
        logger.info(f"Микс операций: {operation_mix}")

        start_time = time.time()
        end_time = start_time + duration

        # Генерируем пользователей и диалоги для теста
        test_users = []
        for _ in range(num_users):
            user_id = self.generate_user_id()
            # Создаем несколько диалогов для пользователя
            for _ in range(random.randint(1, 3)):
                success, dialog_id, _ = await self.create_dialog(user_id)
                if success:
                    # Добавляем несколько сообщений
                    for _ in range(random.randint(2, 5)):
                        await self.add_message_to_dialog(dialog_id)
            test_users.append(user_id)

        logger.info(f"Подготовлено {len(test_users)} пользователей для теста")

        # Определяем операции для выполнения
        operation_functions = {
            "create_dialog": lambda user_id: self.create_dialog(user_id),
            "get_user_dialogs": self.get_user_dialogs,
            "add_message": lambda dialog_id: self.add_message_to_dialog(dialog_id),
            "get_dialog": self.get_dialog,
        }

        # Генерируем список операций согласно микс
        operations = []
        for user_id in test_users:
            user_dialogs = self.created_dialogs[:]  # Копируем список

            for _ in range(operations_per_user):
                # Выбираем операцию согласно вероятностям
                op_type = random.choices(
                    list(operation_mix.keys()),
                    weights=list(operation_mix.values())
                )[0]

                if op_type == "create_dialog":
                    operations.append((operation_functions["create_dialog"], user_id))
                elif op_type == "get_user_dialogs":
                    operations.append((operation_functions["get_user_dialogs"], user_id))
                elif op_type == "add_message" and user_dialogs:
                    dialog_id = random.choice(user_dialogs)
                    operations.append((operation_functions["add_message"], dialog_id))
                elif op_type == "get_dialog" and user_dialogs:
                    dialog_id = random.choice(user_dialogs)
                    operations.append((operation_functions["get_dialog"], dialog_id))

        # Перемешиваем операции
        random.shuffle(operations)

        # Выполняем операции с ограничением по времени
        results_by_operation = {}
        completed_operations = 0

        async def execute_operation(op_func, arg):
            success, response_time = await op_func(arg)
            return op_func.__name__, success, response_time

        semaphore = asyncio.Semaphore(concurrency)

        async def run_with_semaphore(op_func, arg):
            async with semaphore:
                return await execute_operation(op_func, arg)

        # Создаем задачи и выполняем их
        tasks = []
        for op_func, arg in operations:
            if time.time() > end_time:
                break

            task = asyncio.create_task(run_with_semaphore(op_func, arg))
            tasks.append(task)

        # Собираем результаты
        for task in asyncio.as_completed(tasks):
            try:
                op_name, success, response_time = await task

                if op_name not in results_by_operation:
                    results_by_operation[op_name] = {
                        "response_times": [],
                        "success_count": 0,
                        "failure_count": 0
                    }

                results_by_operation[op_name]["response_times"].append(response_time)
                if success:
                    results_by_operation[op_name]["success_count"] += 1
                else:
                    results_by_operation[op_name]["failure_count"] += 1

                completed_operations += 1

                if completed_operations % 100 == 0:
                    logger.info(f"Выполнено {completed_operations} операций")

            except Exception as e:
                logger.error(f"Error in operation: {e}")

        total_time = time.time() - start_time

        # Анализируем результаты
        test_results = []
        total_operations = 0
        total_success = 0

        for op_name, data in results_by_operation.items():
            response_times = data["response_times"]
            success_count = data["success_count"]
            failure_count = data["failure_count"]

            if response_times:
                avg_time = statistics.mean(response_times)
                min_time = min(response_times)
                max_time = max(response_times)
                p95_time = statistics.quantiles(response_times, n=100)[94] if len(response_times) > 1 else avg_time
                p99_time = statistics.quantiles(response_times, n=100)[98] if len(response_times) > 1 else avg_time
            else:
                avg_time = min_time = max_time = p95_time = p99_time = 0

            result = TestResult(
                operation=op_name,
                success_count=success_count,
                failure_count=failure_count,
                total_time=sum(response_times),
                avg_response_time=avg_time,
                min_response_time=min_time,
                max_response_time=max_time,
                p95_response_time=p95_time,
                p99_response_time=p99_time
            )

            test_results.append(result)
            total_operations += success_count + failure_count
            total_success += success_count

        logger.info(f"Нагрузочный тест завершен за {total_time:.2f} секунд")
        logger.info(f"Всего операций: {total_operations}")
        logger.info(f"Успешных операций: {total_success}")
        logger.info(f"Операций в секунду: {total_operations / total_time if total_time > 0 else 0:.2f}")

        return test_results, {
            "total_operations": total_operations,
            "success_rate": total_success / total_operations if total_operations > 0 else 0,
            "operations_per_second": total_operations / total_time if total_time > 0 else 0,
            "total_time": total_time,
            "concurrency": concurrency
        }

    async def run_redis_direct_test(self, num_operations: int = 10000, concurrency: int = 50):
        """Прямое тестирование Redis"""
        logger.info(f"Прямое тестирование Redis: {num_operations} операций")

        start_time = time.time()

        # Генерируем тестовые данные
        test_keys = [f"test_key_{i}" for i in range(1000)]
        test_values = [json.dumps({"data": fake.text()}) for _ in range(1000)]

        async def redis_set_operation(_):
            key = random.choice(test_keys)
            value = random.choice(test_values)
            start = time.time()
            try:
                self.redis.set(key, value)
                return True, time.time() - start
            except Exception as e:
                logger.error(f"Redis set error: {e}")
                return False, time.time() - start

        async def redis_get_operation(_):
            key = random.choice(test_keys)
            start = time.time()
            try:
                self.redis.get(key)
                return True, time.time() - start
            except Exception as e:
                logger.error(f"Redis get error: {e}")
                return False, time.time() - start

        async def redis_hset_operation(_):
            key = f"hash_{random.randint(1, 100)}"
            field = f"field_{random.randint(1, 1000)}"
            value = fake.sentence()
            start = time.time()
            try:
                self.redis.hset(key, field, value)
                return True, time.time() - start
            except Exception as e:
                logger.error(f"Redis hset error: {e}")
                return False, time.time() - start

        # Выполняем операции
        operations = []
        for i in range(num_operations):
            op_type = random.choice(["set", "get", "hset"])
            if op_type == "set":
                operations.append(redis_set_operation)
            elif op_type == "get":
                operations.append(redis_get_operation)
            else:
                operations.append(redis_hset_operation)

        # Выполняем с конкурентностью
        semaphore = asyncio.Semaphore(concurrency)
        response_times = []
        success_count = 0

        async def run_op(op_func, idx):
            async with semaphore:
                return await op_func(idx)

        tasks = [run_op(op, i) for i, op in enumerate(operations)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, tuple):
                success, resp_time = result
                response_times.append(resp_time)
                if success:
                    success_count += 1

        total_time = time.time() - start_time

        if response_times:
            avg_time = statistics.mean(response_times)
            min_time = min(response_times)
            max_time = max(response_times)
            p95_time = statistics.quantiles(response_times, n=100)[94] if len(response_times) > 1 else avg_time
        else:
            avg_time = min_time = max_time = p95_time = 0

        logger.info(f"Redis тест завершен за {total_time:.2f} секунд")
        logger.info(f"Операций в секунду: {num_operations / total_time if total_time > 0 else 0:.2f}")
        logger.info(f"Среднее время ответа: {avg_time * 1000:.2f} мс")
        logger.info(f"p95 время ответа: {p95_time * 1000:.2f} мс")

        return {
            "total_operations": num_operations,
            "successful_operations": success_count,
            "total_time": total_time,
            "operations_per_second": num_operations / total_time if total_time > 0 else 0,
            "avg_response_time_ms": avg_time * 1000,
            "p95_response_time_ms": p95_time * 1000,
            "min_response_time_ms": min_time * 1000,
            "max_response_time_ms": max_time * 1000
        }


def print_results(results, title):
    """Печать результатов в читаемом формате"""
    print(f"\n{'=' * 60}")
    print(f"{title}")
    print(f"{'=' * 60}")

    if isinstance(results, list):
        for result in results:
            print(f"\nОперация: {result.operation}")
            print(f"  Успешно: {result.success_count}")
            print(f"  Неудачно: {result.failure_count}")
            if result.success_count + result.failure_count > 0:
                print(
                    f"  Успешность: {result.success_count / (result.success_count + result.failure_count) * 100:.1f}%")
            print(f"  Среднее время: {result.avg_response_time * 1000:.2f} мс")
            print(f"  Минимальное: {result.min_response_time * 1000:.2f} мс")
            print(f"  Максимальное: {result.max_response_time * 1000:.2f} мс")
            print(f"  p95: {result.p95_response_time * 1000:.2f} мс")
            print(f"  p99: {result.p99_response_time * 1000:.2f} мс")
    elif isinstance(results, dict):
        for key, value in results.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")


async def main():
    parser = argparse.ArgumentParser(description='Load testing for Dialog Microservice')
    parser.add_argument('--mode', type=str, default='populate',
                        choices=['populate', 'loadtest', 'redistest', 'all'],
                        help='Режим работы')
    parser.add_argument('--dialogs', type=int, default=1000,
                        help='Количество диалогов для создания')
    parser.add_argument('--messages', type=int, default=20,
                        help='Сообщений на диалог')
    parser.add_argument('--users', type=int, default=100,
                        help='Количество пользователей для нагрузочного теста')
    parser.add_argument('--operations', type=int, default=50,
                        help='Операций на пользователя')
    parser.add_argument('--concurrency', type=int, default=20,
                        help='Уровень конкурентности')
    parser.add_argument('--duration', type=int, default=60,
                        help='Длительность нагрузочного теста в секундах')
    parser.add_argument('--url', type=str, default=os.environ.get("APP_URL", "http://localhost:8000"),
                        help='URL микросервиса')
    parser.add_argument('--redis-url', type=str, default=os.environ.get("REDIS_URL", "redis://localhost:6379"),
                        help='URL Redis')

    args = parser.parse_args()

    tester = DialogLoadTester(args.url, args.redis_url)

    try:
        await tester.init()

        if args.mode in ['populate', 'all']:
            print("\n" + "=" * 60)
            print("НАПОЛНЕНИЕ БАЗЫ ДАННЫХ")
            print("=" * 60)

            populate_results = await tester.populate_database(
                num_dialogs=args.dialogs,
                messages_per_dialog=args.messages,
                concurrency=args.concurrency
            )
            print_results(populate_results, "Результаты наполнения базы")

        if args.mode in ['loadtest', 'all']:
            print("\n" + "=" * 60)
            print("НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ")
            print("=" * 60)

            # Микс операций: создание диалогов, получение, добавление сообщений
            operation_mix = {
                "create_dialog": 0.2,
                "get_user_dialogs": 0.3,
                "add_message": 0.3,
                "get_dialog": 0.2
            }

            load_test_results, summary = await tester.run_load_test(
                num_users=args.users,
                operations_per_user=args.operations,
                operation_mix=operation_mix,
                concurrency=args.concurrency,
                duration=args.duration
            )

            print_results(load_test_results, "Результаты нагрузочного теста")
            print("\nСводка:")
            print_results(summary, "Общая статистика")

        if args.mode in ['redistest', 'all']:
            print("\n" + "=" * 60)
            print("ТЕСТИРОВАНИЕ REDIS")
            print("=" * 60)

            redis_results = await tester.run_redis_direct_test(
                num_operations=args.dialogs * 10,
                concurrency=args.concurrency * 2
            )
            print_results(redis_results, "Результаты тестирования Redis")

        # Сохраняем статистику в файл
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        with open(f"load_test_results_{timestamp}.json", "w") as f:
            json.dump({
                "timestamp": timestamp,
                "args": vars(args),
                "created_dialogs_count": len(tester.created_dialogs),
                "created_users_count": len(tester.created_users)
            }, f, indent=2)

        logger.info(f"Результаты сохранены в load_test_results_{timestamp}.json")

    except Exception as e:
        logger.error(f"Error during testing: {e}")
        raise
    finally:
        await tester.close()


if __name__ == "__main__":
    asyncio.run(main())
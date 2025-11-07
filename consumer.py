import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def consume_messages():
    """Консумер слушает топик 'events' и обрабатывает сообщения"""

    consumer = AIOKafkaConsumer(
        'events',
        bootstrap_servers='kafka:9092',
        group_id='fastapi-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    await consumer.start()
    logger.info("Consumer started, waiting for messages...")

    try:
        async for message in consumer:
            # Обрабатываем сообщение
            event = message.value
            timestamp = datetime.now().isoformat()

            logger.info(f"[{timestamp}] New event received:")
            logger.info(f"  User: {event['user_id']}")
            logger.info(f"  Action: {event['action']}")
            logger.info(f"  Description: {event['description']}")
            logger.info(f"  Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
            logger.info("-" * 50)

            # Здесь можно добавить обработку события
            # Например, сохранение в БД, отправка уведомления и т.д.

    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume_messages())

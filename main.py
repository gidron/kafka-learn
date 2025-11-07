from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


producer = AIOKafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    logger.info("Kafka Producer started")
    yield
    await producer.stop()
    logger.info("Kafka Producer stopped")

app = FastAPI(title="Kafka Producer API", version="1.0.0", lifespan=lifespan)

class Message(BaseModel):
    user_id: str
    action: str
    description: str

@app.get("/")
async def root():
    return {
        "message": "Добро пожаловать в Kafka Producer API!",
        "endpoint": "/docs для Swagger UI",
        "example": {
            "user_id": "user123",
            "action": "login",
            "description": "User logged in from browser"
        }
    }

@app.post("/send-event")
async def send_event(message: Message):
    """Отправляет событие в Kafka топик 'events'"""
    try:
        # Отправляем сообщение в топик
        await producer.send_and_wait(
            "events",
            value=message.model_dump()
        )
        logger.info(f"Event sent: {message}")
        return {
            "status": "success",
            "message": "Event sent to Kafka",
            "data": message
        }
    except Exception as e:
        logger.error(f"Error sending event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

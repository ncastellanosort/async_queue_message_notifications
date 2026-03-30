import psycopg2
from fastapi import FastAPI
import redis
import json
import os

app = FastAPI()

redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

@app.post("/notify")
def notify(user_id: int, message: str):
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Guardar en DB
    cur.execute(
        "INSERT INTO notifications (user_id, message, status) VALUES (%s, %s, %s) RETURNING id;",
        (user_id, message, "pending")
    )
    notification_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    # 2. Enviar a Redis
    payload = {
        "id": notification_id,
        "user_id": user_id,
        "message": message
    }

    redis_client.rpush("queue", json.dumps(payload))

    return {"status": "queued", "id": notification_id}

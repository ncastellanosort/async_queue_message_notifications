import psycopg2
from fastapi import FastAPI
import redis
import json
import os

app = FastAPI()

redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

def check_postgres():
    try:
        conn = psycopg2.connect(
            host="postgres",
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            connect_timeout=2
        )
        conn.close()
        return True
    except Exception:
        return False


def check_redis():
    try:
        r = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379, socket_connect_timeout=2)
        return r.ping()
    except Exception:
        return False


@app.get("/health")
def health():
    postgres_ok = check_postgres()
    redis_ok = check_redis()

    if postgres_ok and redis_ok:
        return {
            "status": "ok"
        }

    return {
        "status": "error",
        "postgres": postgres_ok,
        "redis": redis_ok
    }

@app.post("/notify")
def notify(user_id: int, message: str):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO notifications (user_id, message, status) VALUES (%s, %s, %s) RETURNING id;",
        (user_id, message, "pending")
    )
    notification_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    payload = {
        "id": notification_id,
        "user_id": user_id,
        "message": message
    }

    redis_client.rpush("queue", json.dumps(payload))

    return {"status": "queued", "id": notification_id}

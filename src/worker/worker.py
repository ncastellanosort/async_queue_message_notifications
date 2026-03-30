import redis
import time
import json
import os
import psycopg2

redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

while True:
    _, data = redis_client.blpop("queue")
    task = json.loads(data)

    print(f"Processing: {task}")

    time.sleep(3)

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        "UPDATE notifications SET status = %s WHERE id = %s",
        ("completed", task["id"])
    )

    conn.commit()
    cur.close()
    conn.close()

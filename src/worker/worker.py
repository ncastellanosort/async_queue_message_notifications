import redis
import time
import json
import os
import psycopg2
import logging
import sys
from datetime import datetime

class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG:    "\033[36m",   # cyan
        logging.INFO:     "\033[32m",   # green
        logging.WARNING:  "\033[33m",   # yellow
        logging.ERROR:    "\033[31m",   # red
        logging.CRITICAL: "\033[1;31m", # bold red
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        record.levelname = f"{color}{record.levelname:<8}{self.RESET}"
        return super().format(record)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColorFormatter(
    fmt="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

logger = logging.getLogger("worker")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

redis_client = redis.Redis(host=os.getenv("REDIS_HOST"), port=6379)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

logger.info("Worker iniciado — esperando tareas en la cola...")

while True:
    try:
        logger.debug("Escuchando en 'queue'...")
        _, data = redis_client.blpop("queue")
        task = json.loads(data)
        logger.info(f"Tarea recibida   → id={task['id']}")

        logger.debug(f"Payload completo → {task}")
        logger.info(f"Procesando       → id={task['id']} (simulando 3s...)")
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

        logger.info(f"Tarea completada → id={task['id']} status=completed ✓")

    except redis.exceptions.ConnectionError as e:
        logger.error(f"Redis no disponible: {e} — reintentando en 3s...")
        time.sleep(3)

    except psycopg2.Error as e:
        logger.error(f"Error en Postgres → id={task.get('id', '?')} | {e}")

    except json.JSONDecodeError as e:
        logger.warning(f"Payload inválido, descartando tarea | {e}")

    except Exception as e:
        logger.critical(f"Error inesperado: {e}", exc_info=True)

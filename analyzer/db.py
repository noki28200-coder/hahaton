"""Вспомогательные функции для работы с PostgreSQL."""

import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_conn():
    """Новое соединение с PostgreSQL."""
    conn = psycopg2.connect(
        os.getenv("POSTGRES_DSN"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    conn.autocommit = False
    return conn


def log_db(conn, call_id: str, level: str, stage: str, message: str, details: dict = None):
    """Записать событие в таблицу call_logs."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO call_logs (call_id, level, stage, message, details) VALUES (%s, %s, %s, %s, %s)",
                (
                    call_id, level, stage, message,
                    psycopg2.extras.Json(details) if details else None,
                ),
            )
        conn.commit()
    except Exception as exc:
        logger.error("Не удалось записать лог в БД: %s", exc)

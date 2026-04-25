"""
Сервис скачивания записей звонков из МангоОфис API.

Алгоритм каждого цикла опроса (каждые POLL_INTERVAL_SECONDS):
  1. Повторить попытки для записей со статусом failed_download
  2. Получить список новых записей из МангоОфис API
  3. Для каждой новой записи:
     - Создать запись в PostgreSQL (status=pending)
     - Скачать MP3 из МангоОфис
     - Загрузить в MinIO
     - Обновить PostgreSQL (status=stored)
     - Отправить событие в Kafka: calls-ready
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import boto3
import httpx
import psycopg2
import psycopg2.extras
from botocore.client import Config
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from kafka.errors import KafkaError

from db import get_conn, log_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [mango] %(levelname)s: %(message)s")
logger = logging.getLogger("mango-collector")

# ── Конфигурация ──────────────────────────────────────────────
MANGO_API_URL = os.getenv("MANGO_API_URL", "https://app.mango-office.ru/vpbx")
MANGO_API_KEY = os.getenv("MANGO_API_KEY", "")
MANGO_API_SALT = os.getenv("MANGO_API_SALT", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "10"))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
BUCKET = os.getenv("MINIO_BUCKET", "call-audios")


# ── Подпись запросов к МангоОфис ─────────────────────────────

def _sign(json_body: str) -> str:
    raw = MANGO_API_KEY + json_body + MANGO_API_SALT
    return hashlib.sha256(raw.encode()).hexdigest()


def mango_post(client: httpx.Client, endpoint: str, params: dict) -> dict:
    json_body = json.dumps(params)
    resp = client.post(
        f"{MANGO_API_URL}/{endpoint.lstrip('/')}",
        data={"vpbx_api_key": MANGO_API_KEY, "sign": _sign(json_body), "json": json_body},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def download_recording(client: httpx.Client, recording_id: str) -> bytes:
    json_body = json.dumps({"recording_id": recording_id})
    resp = client.post(
        f"{MANGO_API_URL}/stats/calls/recording/file/",
        data={"vpbx_api_key": MANGO_API_KEY, "sign": _sign(json_body), "json": json_body},
        timeout=120,
    )
    resp.raise_for_status()
    return resp.content


# ── Инициализация клиентов ────────────────────────────────────

def init_kafka(retries: int = 20, delay: int = 3) -> KafkaProducer:
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
            )
            logger.info("Kafka подключена: %s", KAFKA_BOOTSTRAP)
            return producer
        except KafkaError as e:
            logger.warning("Kafka попытка %d/%d: %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Kafka")


def init_minio():
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )
    try:
        client.create_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            raise
    return client


# ── Загрузка и сохранение одной записи ───────────────────────

def _upload_and_store(
    call_id: str,
    recording_id: str,
    http: httpx.Client,
    minio,
    producer: KafkaProducer,
    db: psycopg2.extensions.connection,
    extra_meta: dict,
):
    """Скачать запись из МангоОфис, загрузить в MinIO, обновить БД, отправить в Kafka."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    object_key = f"calls/{today}/{call_id}.mp3"

    audio_bytes = download_recording(http, recording_id)

    minio.put_object(
        Bucket=BUCKET,
        Key=object_key,
        Body=audio_bytes,
        ContentType="audio/mpeg",
    )

    with db.cursor() as cur:
        cur.execute(
            """UPDATE calls
               SET status = 'stored', file_path = %s, file_size_bytes = %s
               WHERE call_id = %s""",
            (object_key, len(audio_bytes), call_id),
        )
    db.commit()

    event = {
        "call_id": call_id,
        "file_path": object_key,
        "source": "mango",
        "recording_id": recording_id,
        **extra_meta,
        "size_bytes": len(audio_bytes),
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
    }
    producer.send("calls-ready", value=event)
    producer.flush()

    log_db(db, call_id, "info", "download", f"Запись скачана и загружена ({len(audio_bytes)} байт)")
    logger.info("[%s] Загружен (%d байт)", call_id, len(audio_bytes))


# ── Повторные попытки для failed_download ─────────────────────

def retry_failed(http: httpx.Client, minio, producer: KafkaProducer, db):
    with db.cursor() as cur:
        cur.execute(
            """SELECT call_id, mango_recording_id,
                      employee_id, employee_name, phone_from, phone_to,
                      direction, duration_seconds, start_time
               FROM calls
               WHERE status = 'failed_download'
                 AND mango_recording_id IS NOT NULL
                 AND updated_at < NOW() - INTERVAL '5 minutes'
               LIMIT 10""",
        )
        rows = cur.fetchall()

    for row in rows:
        call_id = row["call_id"]
        logger.info("[%s] Повторная попытка скачивания...", call_id)
        try:
            meta = {
                "employee_id": row["employee_id"],
                "employee_name": row["employee_name"],
                "phone_from": row["phone_from"],
                "phone_to": row["phone_to"],
                "direction": row["direction"],
                "duration": row["duration_seconds"],
                "start_time": row["start_time"].isoformat() if row["start_time"] else None,
            }
            _upload_and_store(call_id, row["mango_recording_id"], http, minio, producer, db, meta)
        except Exception as exc:
            logger.error("[%s] Повторная попытка провалилась: %s", call_id, exc)
            log_db(db, call_id, "error", "download", f"Retry failed: {exc}")


# ── Основной цикл опроса ──────────────────────────────────────

def poll_once(http: httpx.Client, minio, producer: KafkaProducer, db):
    # 1. Ретрай упавших загрузок
    retry_failed(http, minio, producer, db)

    # 2. Запрос новых записей из МангоОфис
    now = datetime.now(timezone.utc)
    extra_min = POLL_INTERVAL // 60 + 1
    date_from = int((now - timedelta(minutes=LOOKBACK_MINUTES + extra_min)).timestamp())
    date_to = int(now.timestamp())

    try:
        data = mango_post(http, "stats/calls/records/", {
            "date_from": date_from,
            "date_to": date_to,
            "fields": (
                "start,finish,duration,from_number,to_number,"
                "is_recorded,recording,emp_id,emp_name,call_direction"
            ),
        })
    except httpx.HTTPError as exc:
        logger.error("МангоОфис API ошибка: %s", exc)
        log_db(db, None, "error", "download", f"MangoOffice API error: {exc}")
        return

    results = data.get("results", [])
    new_count = 0

    for group in results:
        for record in group.get("records", []):
            recording_id = record.get("recording")
            if not recording_id:
                continue

            # Проверяем есть ли уже в БД
            with db.cursor() as cur:
                cur.execute(
                    "SELECT call_id FROM calls WHERE mango_recording_id = %s LIMIT 1",
                    (recording_id,),
                )
                if cur.fetchone():
                    continue  # уже обработали

            call_id = f"mango_{recording_id}"
            direction = "inbound" if str(record.get("call_direction", "0")) == "0" else "outbound"
            start_ts = record.get("start", 0)
            start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) if start_ts else None

            meta = {
                "employee_id": str(record.get("emp_id", "")),
                "employee_name": record.get("emp_name", ""),
                "phone_from": record.get("from_number", ""),
                "phone_to": record.get("to_number", ""),
                "direction": direction,
                "duration": int(record.get("duration", 0)),
                "start_time": start_dt.isoformat() if start_dt else None,
            }

            # Создаём запись в БД (status=pending)
            try:
                with db.cursor() as cur:
                    cur.execute(
                        """INSERT INTO calls
                           (call_id, mango_recording_id, source,
                            employee_id, employee_name, phone_from, phone_to,
                            direction, duration_seconds, start_time, status)
                           VALUES (%s,%s,'mango',%s,%s,%s,%s,%s,%s,%s,'pending')
                           ON CONFLICT (call_id) DO NOTHING""",
                        (
                            call_id, recording_id,
                            meta["employee_id"], meta["employee_name"],
                            meta["phone_from"], meta["phone_to"],
                            meta["direction"], meta["duration"],
                            start_dt,
                        ),
                    )
                db.commit()
            except Exception as exc:
                logger.error("[%s] Не удалось создать запись в БД: %s", call_id, exc)
                db.rollback()
                continue

            # Скачиваем и сохраняем
            try:
                _upload_and_store(call_id, recording_id, http, minio, producer, db, meta)
                new_count += 1
            except Exception as exc:
                logger.error("[%s] Ошибка загрузки: %s", call_id, exc)
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE calls SET status = 'failed_download' WHERE call_id = %s",
                        (call_id,),
                    )
                db.commit()
                log_db(db, call_id, "error", "download", f"Ошибка загрузки: {exc}")

    if new_count:
        logger.info("Обработано новых записей: %d", new_count)
    else:
        logger.info("Новых записей нет")


def main():
    if not MANGO_API_KEY or not MANGO_API_SALT:
        logger.error("MANGO_API_KEY и MANGO_API_SALT обязательны. Проверь .env")
        raise SystemExit(1)

    producer = init_kafka()
    minio = init_minio()
    db = get_conn()

    logger.info("Старт. Опрос каждые %ds, глубина %d мин", POLL_INTERVAL, LOOKBACK_MINUTES)

    with httpx.Client() as http:
        while True:
            try:
                poll_once(http, minio, producer, db)
            except psycopg2.InterfaceError:
                # Переподключение к БД при обрыве соединения
                logger.warning("Переподключение к PostgreSQL...")
                db = get_conn()
            except Exception as exc:
                logger.error("Неожиданная ошибка: %s", exc)
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

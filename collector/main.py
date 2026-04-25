"""
HTTP API для ручной загрузки аудиофайлов.

DEPRECATED: Используй POST /calls/upload из сервиса api/.
Этот сервис оставлен для обратной совместимости.
Новые функции добавляются в api/main.py.
"""

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime

import boto3
import psycopg2
import psycopg2.extras
from botocore.client import Config
from botocore.exceptions import ClientError
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from kafka import KafkaProducer
from kafka.errors import KafkaError

from db import get_conn, log_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [collector] %(levelname)s: %(message)s")
logger = logging.getLogger("collector")

BUCKET = os.getenv("MINIO_BUCKET", "call-audios")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

_minio = None
_producer = None


def _init_minio():
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
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


def _init_kafka(retries: int = 15, delay: int = 3) -> KafkaProducer:
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _minio, _producer
    _minio = _init_minio()
    _producer = _init_kafka()
    yield
    if _producer:
        _producer.flush()
        _producer.close()


app = FastAPI(title="Call Collector", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "service": "collector"}


@app.post("/upload_call")
async def upload_call(
    file: UploadFile = File(...),
    employee_id: str = Form(default=""),
    employee_name: str = Form(default=""),
    phone_from: str = Form(default=""),
    phone_to: str = Form(default=""),
    direction: str = Form(default=""),
):
    is_audio = file.content_type and "audio" in file.content_type
    is_mp3 = file.filename and file.filename.lower().endswith(".mp3")
    if not is_audio and not is_mp3:
        raise HTTPException(status_code=400, detail="Принимаются только аудио MP3 файлы")

    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail="Файл пустой")

    call_id = str(uuid.uuid4())
    today = datetime.utcnow().strftime("%Y-%m-%d")
    object_key = f"calls/{today}/{call_id}.mp3"

    _minio.put_object(Bucket=BUCKET, Key=object_key, Body=contents, ContentType="audio/mpeg")

    # Пишем в PostgreSQL
    try:
        db = get_conn()
        with db.cursor() as cur:
            cur.execute(
                """INSERT INTO calls
                   (call_id, source, employee_id, employee_name, phone_from, phone_to,
                    direction, file_path, file_size_bytes, status)
                   VALUES (%s,'manual',%s,%s,%s,%s,%s,%s,%s,'stored')""",
                (
                    call_id, employee_id or None, employee_name or None,
                    phone_from or None, phone_to or None, direction or None,
                    object_key, len(contents),
                ),
            )
        db.commit()
        log_db(db, call_id, "info", "upload", "Файл загружен вручную через collector")
        db.close()
    except Exception as exc:
        logger.error("[%s] Ошибка записи в БД: %s", call_id, exc)

    event = {
        "call_id": call_id,
        "file_path": object_key,
        "source": "manual",
        "employee_id": employee_id,
        "employee_name": employee_name,
        "phone_from": phone_from,
        "phone_to": phone_to,
        "direction": direction,
        "size_bytes": len(contents),
        "uploaded_at": datetime.utcnow().isoformat() + "Z",
    }
    _producer.send("calls-ready", value=event)
    _producer.flush()

    logger.info("[%s] Загружен (%d байт)", call_id, len(contents))
    return {"status": "uploaded", "call_id": call_id, "file_path": object_key}

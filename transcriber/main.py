"""
Сервис транскрибации звонков — Kafka consumer + faster-whisper.

Pipeline:
  Kafka 'calls-ready'
    → скачать MP3 из MinIO
    → предобработать аудио (PyAV)
    → транскрибировать (WhisperModel, загружается ОДИН РАЗ)
    → обновить PostgreSQL (transcript_text, status='transcribed')
    → отправить в Kafka 'transcriptions-done'
"""

import json
import logging
import os
import time

import boto3
import psycopg2.extras
from botocore.client import Config
from faster_whisper import WhisperModel
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from audio_processing import prepare_audio_for_whisper_bytes
from db import get_conn, log_db
from transcription import transcribe_audio

logging.basicConfig(level=logging.INFO, format="%(asctime)s [transcriber] %(levelname)s: %(message)s")
logger = logging.getLogger("transcriber")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
BUCKET = os.getenv("MINIO_BUCKET", "call-audios")
WHISPER_MODEL_NAME = os.getenv("WHISPER_MODEL", "medium")
WHISPER_LANGUAGE = os.getenv("WHISPER_LANGUAGE", "ru")
WHISPER_THREADS = int(os.getenv("WHISPER_THREADS", "4"))


def wait_for_kafka(retries: int = 20, delay: int = 3):
    for attempt in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
            p.close()
            logger.info("Kafka готова: %s", KAFKA_BOOTSTRAP)
            return
        except KafkaError as e:
            logger.warning("Kafka ожидание %d/%d: %s", attempt + 1, retries, e)
            time.sleep(delay)
    raise RuntimeError("Не удалось подключиться к Kafka")


def main():
    wait_for_kafka()

    minio = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
    )

    db = get_conn()

    logger.info("Загружаю Whisper модель '%s'...", WHISPER_MODEL_NAME)
    model = WhisperModel(
        WHISPER_MODEL_NAME,
        device="cpu",
        compute_type="int8",
        cpu_threads=WHISPER_THREADS,
        num_workers=1,
    )
    logger.info("Whisper готов.")

    consumer = KafkaConsumer(
        "calls-ready",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="transcriber-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )

    logger.info("Слушаю топик 'calls-ready'...")

    for msg in consumer:
        event = msg.value
        call_id = event.get("call_id") or event.get("file_id", "unknown")
        file_path = event.get("file_path") or event.get("file_id", "")
        logger.info("[%s] Транскрибирую...", call_id)

        try:
            resp = minio.get_object(Bucket=BUCKET, Key=file_path)
            audio_bytes = resp["Body"].read()

            audio_data = prepare_audio_for_whisper_bytes(audio_bytes)
            transcript = transcribe_audio(model, audio_data, language=WHISPER_LANGUAGE)

            # Обновляем PostgreSQL
            try:
                with db.cursor() as cur:
                    cur.execute(
                        """UPDATE calls SET
                               status = 'transcribed',
                               transcript_text = %s,
                               transcript_language = %s,
                               transcript_language_probability = %s,
                               transcript_segments = %s,
                               transcribed_at = NOW()
                           WHERE call_id = %s""",
                        (
                            transcript["full_text"],
                            transcript["language"],
                            transcript["language_probability"],
                            psycopg2.extras.Json(transcript["segments"]),
                            call_id,
                        ),
                    )
                db.commit()
                log_db(
                    db, call_id, "info", "transcription",
                    f"Транскрибировано: {len(transcript['segments'])} сегментов",
                )
            except Exception as db_exc:
                logger.error("[%s] Ошибка записи транскрипции в БД: %s", call_id, db_exc)
                try:
                    db.rollback()
                except Exception:
                    db = get_conn()

            # Отправляем в Kafka
            out_event = {**event, "transcript": transcript}
            producer.send("transcriptions-done", value=out_event)
            producer.flush()

            logger.info(
                "[%s] Готово: %d сег., язык=%s (p=%.2f)",
                call_id, len(transcript["segments"]),
                transcript["language"], transcript["language_probability"],
            )

        except Exception as exc:
            logger.error("[%s] ОШИБКА: %s", call_id, exc)

            # Обновляем статус в БД
            try:
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE calls SET status='failed_transcription' WHERE call_id=%s",
                        (call_id,),
                    )
                db.commit()
                log_db(db, call_id, "error", "transcription", f"Ошибка: {exc}")
            except Exception:
                try:
                    db.rollback()
                    db = get_conn()
                except Exception:
                    pass

            producer.send("calls-errors", value={**event, "error": str(exc), "stage": "transcription"})
            producer.flush()


if __name__ == "__main__":
    main()

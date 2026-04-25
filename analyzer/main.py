"""
Сервис анализа транскрипций — Kafka consumer + Ollama/Gemma.

Pipeline:
  Kafka 'transcriptions-done'
    → сформировать промпт
    → вызвать Ollama API (Gemma)
    → обновить PostgreSQL (analysis_*, status='analyzed')
    → отправить в Kafka 'analysis-done'
    → опционально: POST вебхук в CRM
"""

import json
import logging
import os
import time

import httpx
import psycopg2.extras
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from db import get_conn, log_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [analyzer] %(levelname)s: %(message)s")
logger = logging.getLogger("analyzer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
CRM_WEBHOOK_URL = os.getenv("CRM_WEBHOOK_URL", "")

# ── Промпт для анализа ────────────────────────────────────────

ANALYSIS_PROMPT = """\
Ты — специалист по анализу качества звонков в колл-центре.
Проанализируй транскрипцию телефонного разговора между сотрудником и клиентом.

ИНФОРМАЦИЯ О ЗВОНКЕ:
- Сотрудник: {employee_name} (ID: {employee_id})
- Направление: {direction}
- Длительность: {duration} сек

ТРАНСКРИПЦИЯ:
{transcript}

Ответь СТРОГО в формате JSON без markdown и лишнего текста:
{{
  "summary": "краткое резюме звонка в 2-3 предложения",
  "sentiment": "positive или neutral или negative",
  "script_compliance": true,
  "script_violations": ["список нарушений, пустой массив если нарушений нет"],
  "action_items": ["список действий после звонка"],
  "score": 8,
  "topics": ["ключевые темы разговора"],
  "call_outcome": "sold или callback или rejected или escalated или consultation или other"
}}"""


# ── Ollama ────────────────────────────────────────────────────

def pull_model():
    """Скачать модель если не загружена."""
    logger.info("Проверяю наличие модели '%s'...", OLLAMA_MODEL)
    with httpx.Client(timeout=600) as client:
        try:
            r = client.post(
                f"{OLLAMA_HOST}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": "hi", "stream": False},
                timeout=15,
            )
            if r.status_code == 200:
                logger.info("Модель '%s' готова.", OLLAMA_MODEL)
                return
        except Exception:
            pass

        logger.info("Скачиваю модель '%s' (может занять несколько минут)...", OLLAMA_MODEL)
        with client.stream("POST", f"{OLLAMA_HOST}/api/pull", json={"name": OLLAMA_MODEL}) as r:
            for line in r.iter_lines():
                if line:
                    try:
                        d = json.loads(line)
                        status = d.get("status", "")
                        if any(k in status for k in ("pulling", "verifying", "success", "error")):
                            logger.info("[Ollama] %s", status)
                    except Exception:
                        pass
    logger.info("Модель '%s' готова.", OLLAMA_MODEL)


def analyze_with_ollama(prompt: str) -> dict:
    """Отправить промпт в Ollama, вернуть распарсенный JSON."""
    with httpx.Client(timeout=180) as client:
        resp = client.post(
            f"{OLLAMA_HOST}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
        )
        resp.raise_for_status()
        return json.loads(resp.json()["response"])


# ── CRM вебхук ────────────────────────────────────────────────

def send_webhook(payload: dict):
    if not CRM_WEBHOOK_URL:
        return
    try:
        with httpx.Client(timeout=10) as client:
            client.post(CRM_WEBHOOK_URL, json=payload)
        logger.info("Вебхук отправлен: %s", CRM_WEBHOOK_URL)
    except Exception as exc:
        logger.warning("Вебхук ошибка: %s", exc)


# ── Kafka ─────────────────────────────────────────────────────

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
    pull_model()
    wait_for_kafka()

    db = get_conn()

    consumer = KafkaConsumer(
        "transcriptions-done",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="analyzer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        session_timeout_ms=60000,
        heartbeat_interval_ms=20000,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )

    logger.info("Слушаю топик 'transcriptions-done'...")

    for msg in consumer:
        event = msg.value
        call_id = event.get("call_id", "unknown")
        logger.info("[%s] Анализирую...", call_id)

        transcript = event.get("transcript", {})
        full_text = transcript.get("full_text", "")

        if not full_text.strip():
            logger.warning("[%s] Пустая транскрипция, пропускаю", call_id)
            continue

        try:
            prompt = ANALYSIS_PROMPT.format(
                employee_name=event.get("employee_name") or "неизвестен",
                employee_id=event.get("employee_id") or "",
                direction=event.get("direction") or "не указано",
                duration=event.get("duration") or 0,
                transcript=full_text,
            )

            analysis = analyze_with_ollama(prompt)

            # Обновляем PostgreSQL
            try:
                with db.cursor() as cur:
                    cur.execute(
                        """UPDATE calls SET
                               status = 'analyzed',
                               analysis_summary = %s,
                               analysis_sentiment = %s,
                               analysis_script_compliance = %s,
                               analysis_script_violations = %s,
                               analysis_action_items = %s,
                               analysis_score = %s,
                               analysis_topics = %s,
                               analysis_call_outcome = %s,
                               analyzed_at = NOW()
                           WHERE call_id = %s""",
                        (
                            analysis.get("summary"),
                            analysis.get("sentiment"),
                            bool(analysis.get("script_compliance")),
                            psycopg2.extras.Json(analysis.get("script_violations") or []),
                            psycopg2.extras.Json(analysis.get("action_items") or []),
                            analysis.get("score"),
                            psycopg2.extras.Json(analysis.get("topics") or []),
                            analysis.get("call_outcome"),
                            call_id,
                        ),
                    )
                db.commit()
                log_db(
                    db, call_id, "info", "analysis",
                    f"Анализ завершён: score={analysis.get('score')}, sentiment={analysis.get('sentiment')}",
                )
            except Exception as db_exc:
                logger.error("[%s] Ошибка записи анализа в БД: %s", call_id, db_exc)
                try:
                    db.rollback()
                except Exception:
                    db = get_conn()

            # Kafka: analysis-done
            out_event = {
                **event,
                "analysis": analysis,
                "analyzed_at": event.get("uploaded_at", ""),
            }
            producer.send("analysis-done", value=out_event)
            producer.flush()

            # CRM вебхук
            send_webhook({
                "call_id": call_id,
                "employee_id": event.get("employee_id"),
                "employee_name": event.get("employee_name"),
                "phone_from": event.get("phone_from"),
                "phone_to": event.get("phone_to"),
                "duration": event.get("duration"),
                "analysis": analysis,
            })

            score = analysis.get("score", "?")
            sentiment = analysis.get("sentiment", "?")
            outcome = analysis.get("call_outcome", "?")
            logger.info("[%s] Готово: score=%s, sentiment=%s, outcome=%s", call_id, score, sentiment, outcome)

        except Exception as exc:
            logger.error("[%s] ОШИБКА: %s", call_id, exc)

            try:
                with db.cursor() as cur:
                    cur.execute(
                        "UPDATE calls SET status='failed_analysis' WHERE call_id=%s",
                        (call_id,),
                    )
                db.commit()
                log_db(db, call_id, "error", "analysis", f"Ошибка: {exc}")
            except Exception:
                try:
                    db.rollback()
                    db = get_conn()
                except Exception:
                    pass

            producer.send("calls-errors", value={**event, "error": str(exc), "stage": "analysis"})
            producer.flush()


if __name__ == "__main__":
    main()

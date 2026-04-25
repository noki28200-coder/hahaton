#!/bin/bash
set -e

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
MAX_RETRIES=30
RETRY_DELAY=3

echo "[kafka-init] Ожидаю Kafka на $KAFKA_BOOTSTRAP..."
for i in $(seq 1 $MAX_RETRIES); do
    if kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list > /dev/null 2>&1; then
        echo "[kafka-init] Kafka готова"
        break
    fi
    echo "[kafka-init] Попытка $i/$MAX_RETRIES, жду ${RETRY_DELAY}с..."
    sleep $RETRY_DELAY
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "[kafka-init] Kafka недоступна, выхожу"
        exit 1
    fi
done

echo "[kafka-init] Создаю топики..."

kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic calls-ready \
    --partitions 3 \
    --replication-factor 1
echo "[kafka-init] Топик 'calls-ready' готов"

kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic transcriptions-done \
    --partitions 3 \
    --replication-factor 1
echo "[kafka-init] Топик 'transcriptions-done' готов"

kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic analysis-done \
    --partitions 3 \
    --replication-factor 1
echo "[kafka-init] Топик 'analysis-done' готов"

kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic calls-errors \
    --partitions 1 \
    --replication-factor 1
echo "[kafka-init] Топик 'calls-errors' готов"

echo "[kafka-init] Все топики:"
kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list

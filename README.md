# Система анализа звонков

Микросервисная система автоматической обработки записей звонков:
скачивает MP3 из МангоОфис → хранит в MinIO → транскрибирует через Whisper → анализирует через Ollama/Gemma → отдаёт результаты в CRM через REST API.

## Содержание

- [Архитектура](#архитектура)
- [Сервисы](#сервисы)
- [Требования](#требования)
- [Установка на Debian](#установка-на-debian)
- [Настройка .env](#настройка-env)
- [API — полный справочник](#api--полный-справочник)
- [База данных](#база-данных)
- [Kafka-топики](#kafka-топики)
- [Управление и обслуживание](#управление-и-обслуживание)
- [Диагностика и устранение неисправностей](#диагностика-и-устранение-неисправностей)
- [Структура проекта](#структура-проекта)
- [Разработка](#разработка)

---

## Архитектура

```
МангоОфис API
      │  (каждые N минут)
      ▼
mango-collector
      │  скачивает MP3, сохраняет в MinIO, пишет в PostgreSQL
      ▼
  MinIO (MP3) ──► Kafka: calls-ready
                         │
                ┌────────┘
                ▼
         transcriber  (faster-whisper)
                │  пишет транскрипцию в PostgreSQL
                ▼
        Kafka: transcriptions-done
                │
       ┌────────┘
       ▼
   analyzer  (Ollama / Gemma)
       │  пишет анализ в PostgreSQL
       ├──► Kafka: analysis-done
       └──► CRM вебхук (опционально)

               ▲
               │  все данные хранятся в PostgreSQL
               │
  nginx :80 ──► api  (FastAPI)
                 REST API для CRM и фронтенда
```

**Ручная загрузка:**
```
CRM / curl ──► POST /calls/upload ──► MinIO + PostgreSQL + Kafka: calls-ready
                                                   │
                                             тот же пайплайн ──►
```

---

## Сервисы

| Сервис | Порт (хост) | Назначение |
|---|---|---|
| `nginx` | **80** | Точка входа: проксирует API, место для фронтенда |
| `api` | — (внутренний 8000) | Главный REST API — всё взаимодействие с CRM/фронтом |
| `mango-collector` | — | Автоматический опрос МангоОфис API |
| `collector` | — | Устаревший HTTP API загрузки (замещён `/calls/upload`) |
| `transcriber` | — | Транскрибация через faster-whisper |
| `analyzer` | — | LLM-анализ через Ollama/Gemma |
| `minio` | 9000 (API), **9001** (веб-консоль) | Хранилище MP3 файлов |
| `postgres` | — (только внутри сети) | База данных |
| `kafka` | — (внутри сети) | Очередь событий |
| `zookeeper` | — | Координатор Kafka |
| `ollama` | 11434 | LLM-сервер (Gemma) |

---

## Требования

| Компонент | Минимум | Рекомендуется |
|---|---|---|
| RAM | 8 GB | 16 GB |
| CPU | 4 ядра | 8+ ядер |
| Диск | 20 GB | 50 GB+ |
| Docker Engine | 24+ | latest |
| Docker Compose | v2 (встроен в Docker) | — |

Модели (скачиваются автоматически при первом запуске):
- **Whisper medium** — ~1.5 GB
- **Gemma 3 4b** — ~3 GB

---

## Установка на Debian

### Шаг 1. Установить Docker Engine

```bash
# Официальная установка (не docker.io из apt!)
curl -fsSL https://get.docker.com | sh

# Добавить пользователя в группу docker
sudo usermod -aG docker $USER

# Применить группу без перелогина
newgrp docker

# Проверить
docker compose version
# Ожидаемый вывод: Docker Compose version v2.x.x
```

### Шаг 2. Склонировать репозиторий

```bash
git clone https://github.com/<ваш-аккаунт>/calls.git
cd calls
```

### Шаг 3. Настроить переменные окружения

```bash
cp .env.example .env
nano .env
```

Обязательно заполнить (остальное можно оставить по умолчанию):
- `MANGO_API_KEY` — ключ API из личного кабинета МангоОфис
- `MANGO_API_SALT` — соль для подписи из личного кабинета МангоОфис
- `API_KEY` — ключ для авторизации в REST API (см. ниже как сгенерировать)
- `POSTGRES_PASSWORD` — пароль базы данных
- `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` — логин/пароль MinIO

Сгенерировать безопасный API-ключ:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Шаг 4. Первый запуск

```bash
# Запуск всех сервисов в фоне
docker compose up -d

# При первом запуске Docker скачает образы и модели — займёт 10–40 минут
# Наблюдать за прогрессом:
docker compose logs -f
```

### Шаг 5. Проверить работоспособность

```bash
# Статус контейнеров (все должны быть healthy или running)
docker compose ps

# Health check API
curl http://localhost/health
# Ожидается: {"status":"ok","service":"api"}

# Открыть в браузере интерактивную документацию API
# http://localhost/docs

# Открыть в браузере консоль MinIO
# http://localhost:9001
# Логин/пароль из .env: MINIO_ACCESS_KEY / MINIO_SECRET_KEY
```

---

## Настройка .env

```bash
# ── PostgreSQL ─────────────────────────────────────────────────
POSTGRES_DB=callsdb
POSTGRES_USER=callsuser
POSTGRES_PASSWORD=СМЕНИТЬ_НА_СИЛЬНЫЙ_ПАРОЛЬ

# ── MinIO (объектное хранилище для MP3) ────────────────────────
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=СМЕНИТЬ_НА_СИЛЬНЫЙ_ПАРОЛЬ
MINIO_BUCKET=call-audios

# ── REST API авторизация ────────────────────────────────────────
# Генерировать: python3 -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY=ВАШ_СЕКРЕТНЫЙ_КЛЮЧ

# ── МангоОфис ──────────────────────────────────────────────────
MANGO_API_KEY=ваш_ключ_мангоофис
MANGO_API_SALT=ваша_соль_мангоофис
MANGO_API_URL=https://app.mango-office.ru/vpbx

# ── Опрос МангоОфис ────────────────────────────────────────────
POLL_INTERVAL_SECONDS=300     # каждые 5 минут
LOOKBACK_MINUTES=10           # глубина поиска новых записей

# ── Whisper (транскрибация) ─────────────────────────────────────
WHISPER_MODEL=medium          # tiny/base/small/medium/large-v3
WHISPER_LANGUAGE=ru
WHISPER_THREADS=4

# ── Ollama (анализ) ─────────────────────────────────────────────
OLLAMA_MODEL=gemma3:4b        # или gemma3:12b, llama3:8b, etc.

# ── CRM вебхук (опционально) ───────────────────────────────────
CRM_WEBHOOK_URL=              # оставить пустым если не нужен
```

### Whisper модели: сравнение

| Модель | Размер | Скорость | Точность |
|---|---|---|---|
| `tiny` | ~75 MB | очень быстро | низкая |
| `base` | ~150 MB | быстро | средняя |
| `small` | ~480 MB | средне | хорошая |
| `medium` | ~1.5 GB | медленно | высокая (по умолчанию) |
| `large-v3` | ~3 GB | очень медленно | максимальная |

---

## API — полный справочник

Базовый URL: `http://localhost` (через nginx)

**Авторизация:** заголовок `X-API-Key: <ваш API_KEY из .env>`

Интерактивная документация (Swagger UI): `http://localhost/docs`

---

### Health Check

```
GET /health
```
Без авторизации. Используется для мониторинга.

```bash
curl http://localhost/health
```
```json
{"status": "ok", "service": "api"}
```

---

### Загрузка звонка вручную

```
POST /calls/upload
Content-Type: multipart/form-data
X-API-Key: <ключ>
```

Поля формы:
| Поле | Тип | Обязательно | Описание |
|---|---|---|---|
| `file` | файл | да | MP3-файл записи |
| `employee_id` | строка | нет | ID сотрудника |
| `employee_name` | строка | нет | Имя сотрудника |
| `phone_from` | строка | нет | Номер звонящего |
| `phone_to` | строка | нет | Номер принимающего |
| `direction` | строка | нет | `inbound` или `outbound` |

```bash
curl -X POST http://localhost/calls/upload \
  -H "X-API-Key: ВАШ_КЛЮЧ" \
  -F "file=@/path/to/call.mp3" \
  -F "employee_id=42" \
  -F "employee_name=Иван Петров" \
  -F "phone_from=+79001234567" \
  -F "phone_to=+74951234567" \
  -F "direction=inbound"
```
```json
{
  "status": "uploaded",
  "call_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_path": "calls/2024-07-15/550e8400-e29b-41d4-a716-446655440000.mp3"
}
```

---

### Список звонков

```
GET /calls
X-API-Key: <ключ>
```

Query-параметры (все опциональны):
| Параметр | Описание | Пример |
|---|---|---|
| `status` | Фильтр по статусу | `analyzed` |
| `source` | Источник | `mango` или `manual` |
| `employee_id` | ID сотрудника | `42` |
| `date_from` | От даты (ISO 8601) | `2024-01-01` |
| `date_to` | До даты (ISO 8601) | `2024-12-31` |
| `limit` | Кол-во записей (1–500) | `50` |
| `offset` | Сдвиг для пагинации | `0` |

```bash
# Все проанализированные за последний месяц
curl "http://localhost/calls?status=analyzed&date_from=2024-07-01&limit=20" \
  -H "X-API-Key: ВАШ_КЛЮЧ"

# Звонки конкретного сотрудника
curl "http://localhost/calls?employee_id=42&limit=10" \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "total": 142,
  "limit": 20,
  "offset": 0,
  "calls": [
    {
      "call_id": "mango_rec123abc",
      "source": "mango",
      "employee_id": "42",
      "employee_name": "Иван Петров",
      "phone_from": "+79001234567",
      "phone_to": "+74951234567",
      "direction": "inbound",
      "duration_seconds": 245,
      "status": "analyzed",
      "analysis_sentiment": "positive",
      "analysis_score": 8,
      "analysis_call_outcome": "sold",
      "created_at": "2024-07-15T10:30:00Z"
    }
  ]
}
```

---

### Полная информация о звонке

```
GET /calls/{call_id}
X-API-Key: <ключ>
```

`call_id` — внутренний UUID или ID записи МангоОфис (`mango_recording_id`).

```bash
curl http://localhost/calls/mango_rec123abc \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```

Возвращает полную строку из таблицы `calls`, включая все поля транскрипции и анализа.

---

### Статус обработки

```
GET /calls/{call_id}/status
X-API-Key: <ключ>
```

Лёгкий эндпоинт — только статус и временные метки (без текста транскрипции).

```bash
curl http://localhost/calls/mango_rec123abc/status \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "call_id": "mango_rec123abc",
  "mango_recording_id": "rec123abc",
  "status": "analyzed",
  "created_at": "2024-07-15T10:30:00Z",
  "transcribed_at": "2024-07-15T10:35:20Z",
  "analyzed_at": "2024-07-15T10:38:45Z"
}
```

Возможные значения `status`:
| Статус | Описание |
|---|---|
| `pending` | Запись создана в БД, файл ещё скачивается |
| `stored` | MP3 сохранён в MinIO, ожидает транскрибации |
| `transcribed` | Транскрипция готова, ожидает анализа |
| `analyzed` | Анализ завершён |
| `failed_download` | Ошибка скачивания из МангоОфис (будет повторная попытка) |
| `failed_transcription` | Ошибка транскрибации |
| `failed_analysis` | Ошибка анализа |

---

### Скачать аудиозапись

```
GET /calls/{call_id}/audio
X-API-Key: <ключ>
```

Стриминг MP3 напрямую из MinIO.

```bash
# Скачать файл
curl http://localhost/calls/mango_rec123abc/audio \
  -H "X-API-Key: ВАШ_КЛЮЧ" \
  -o call.mp3

# Или открыть прямо в плеере (например через ffplay)
ffplay "http://localhost/calls/mango_rec123abc/audio"
```

---

### Транскрипция

```
GET /calls/{call_id}/transcript
X-API-Key: <ключ>
```

```bash
curl http://localhost/calls/mango_rec123abc/transcript \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "call_id": "mango_rec123abc",
  "language": "ru",
  "language_probability": 0.99,
  "full_text": "Добрый день, компания Апогей, чем могу помочь?...",
  "segments": [
    {
      "id": 0,
      "start": 0.0,
      "end": 2.5,
      "text": "Добрый день, компания Апогей, чем могу помочь?"
    },
    {
      "id": 1,
      "start": 3.1,
      "end": 6.2,
      "text": "Здравствуйте, меня интересует ваш тариф..."
    }
  ],
  "transcribed_at": "2024-07-15T10:35:20Z"
}
```

---

### AI-анализ

```
GET /calls/{call_id}/analysis
X-API-Key: <ключ>
```

```bash
curl http://localhost/calls/mango_rec123abc/analysis \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "call_id": "mango_rec123abc",
  "summary": "Клиент обратился за консультацией по тарифным планам. Сотрудник подробно объяснил условия и предложил подходящий тариф. Договорились о перезвоне для уточнения деталей.",
  "sentiment": "positive",
  "script_compliance": true,
  "script_violations": [],
  "action_items": [
    "Перезвонить клиенту в среду до 15:00",
    "Отправить КП на email"
  ],
  "score": 8,
  "topics": ["тарифы", "подключение", "консультация"],
  "call_outcome": "callback",
  "analyzed_at": "2024-07-15T10:38:45Z",
  "metadata": {
    "employee_id": "42",
    "employee_name": "Иван Петров",
    "duration_seconds": 245,
    "direction": "inbound"
  }
}
```

Возможные значения:
- `sentiment`: `positive`, `neutral`, `negative`
- `call_outcome`: `sold`, `callback`, `rejected`, `escalated`, `consultation`, `other`
- `score`: число от 1 до 10

---

### Статистика для дашборда

```
GET /admin/stats
X-API-Key: <ключ>
```

```bash
curl http://localhost/admin/stats \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "generated_at": "2024-07-15T12:00:00Z",
  "calls": {
    "total": 1542,
    "by_status": {
      "analyzed": 1480,
      "transcribed": 12,
      "stored": 8,
      "failed_analysis": 30,
      "failed_transcription": 5,
      "failed_download": 7
    },
    "last_24h": {
      "downloaded": 48,
      "transcribed": 45,
      "analyzed": 43
    }
  },
  "errors": {
    "last_24h": 5,
    "last_7d": 30
  },
  "storage": {
    "total_size_bytes": 2147483648,
    "total_size_mb": 2048.0
  },
  "last_activity": {
    "last_download": "2024-07-15T11:58:00Z",
    "last_transcription": "2024-07-15T11:59:00Z",
    "last_analysis": "2024-07-15T12:00:00Z",
    "last_error": "2024-07-15T10:15:00Z"
  }
}
```

---

### Журнал операций и ошибок

```
GET /admin/logs
X-API-Key: <ключ>
```

Query-параметры (все опциональны):
| Параметр | Описание |
|---|---|
| `level` | `info`, `warning`, `error` |
| `stage` | `download`, `upload`, `transcription`, `analysis` |
| `call_id` | Фильтр по конкретному звонку |
| `limit` | До 1000 (по умолчанию 100) |
| `offset` | Для пагинации |

```bash
# Только ошибки за всё время
curl "http://localhost/admin/logs?level=error&limit=50" \
  -H "X-API-Key: ВАШ_КЛЮЧ"

# История обработки конкретного звонка
curl "http://localhost/admin/logs?call_id=mango_rec123abc" \
  -H "X-API-Key: ВАШ_КЛЮЧ"

# Ошибки на этапе транскрибации
curl "http://localhost/admin/logs?level=error&stage=transcription" \
  -H "X-API-Key: ВАШ_КЛЮЧ"
```
```json
{
  "total": 8,
  "limit": 100,
  "offset": 0,
  "logs": [
    {
      "id": 1042,
      "call_id": "mango_rec123abc",
      "level": "info",
      "stage": "download",
      "message": "Запись скачана и загружена (1048576 байт)",
      "details": null,
      "created_at": "2024-07-15T10:30:05Z"
    },
    {
      "id": 1043,
      "call_id": "mango_rec123abc",
      "level": "info",
      "stage": "transcription",
      "message": "Транскрибировано: 42 сегментов",
      "details": null,
      "created_at": "2024-07-15T10:35:20Z"
    }
  ]
}
```

---

## База данных

### Таблица `calls`

| Колонка | Тип | Описание |
|---|---|---|
| `call_id` | TEXT UNIQUE | Идентификатор звонка (UUID для manual, `mango_<id>` для МангоОфис) |
| `mango_recording_id` | TEXT UNIQUE | ID записи МангоОфис (NULL для ручной загрузки) |
| `source` | TEXT | `manual` или `mango` |
| `employee_id` | TEXT | ID сотрудника |
| `employee_name` | TEXT | Имя сотрудника |
| `phone_from` | TEXT | Номер звонящего |
| `phone_to` | TEXT | Номер принимающего |
| `direction` | TEXT | `inbound` или `outbound` |
| `duration_seconds` | INT | Длительность в секундах |
| `start_time` | TIMESTAMPTZ | Время начала звонка |
| `file_path` | TEXT | Путь в MinIO (`calls/YYYY-MM-DD/<uuid>.mp3`) |
| `file_size_bytes` | BIGINT | Размер файла в байтах |
| `status` | TEXT | Текущий статус обработки |
| `transcript_text` | TEXT | Полный текст транскрипции |
| `transcript_language` | TEXT | Определённый язык (`ru`, `en`, ...) |
| `transcript_language_probability` | FLOAT | Уверенность в языке (0–1) |
| `transcript_segments` | JSONB | Массив сегментов с таймстампами |
| `transcribed_at` | TIMESTAMPTZ | Время завершения транскрибации |
| `analysis_summary` | TEXT | Краткое резюме |
| `analysis_sentiment` | TEXT | `positive` / `neutral` / `negative` |
| `analysis_script_compliance` | BOOL | Соответствие скрипту |
| `analysis_script_violations` | JSONB | Нарушения скрипта (массив строк) |
| `analysis_action_items` | JSONB | Действия после звонка (массив строк) |
| `analysis_score` | INT | Оценка качества (1–10) |
| `analysis_topics` | JSONB | Ключевые темы (массив строк) |
| `analysis_call_outcome` | TEXT | Итог звонка |
| `analyzed_at` | TIMESTAMPTZ | Время завершения анализа |
| `created_at` | TIMESTAMPTZ | Время создания записи |
| `updated_at` | TIMESTAMPTZ | Время последнего обновления (auto) |

### Таблица `call_logs`

Журнал всех операций по каждому звонку.

| Колонка | Тип | Описание |
|---|---|---|
| `id` | BIGSERIAL | PK |
| `call_id` | TEXT | Ссылка на `calls.call_id` |
| `level` | TEXT | `info`, `warning`, `error` |
| `stage` | TEXT | `download`, `upload`, `transcription`, `analysis` |
| `message` | TEXT | Сообщение |
| `details` | JSONB | Дополнительные данные |
| `created_at` | TIMESTAMPTZ | Время записи |

### Прямой доступ к PostgreSQL

```bash
# Войти в psql
docker compose exec postgres psql -U callsuser -d callsdb

# Примеры запросов:
# Статистика по статусам
SELECT status, COUNT(*) FROM calls GROUP BY status;

# Последние 10 проанализированных
SELECT call_id, employee_name, analysis_score, analysis_sentiment, analyzed_at
FROM calls WHERE status = 'analyzed'
ORDER BY analyzed_at DESC LIMIT 10;

# Все ошибки за сегодня
SELECT * FROM call_logs WHERE level = 'error' AND created_at > NOW() - INTERVAL '24h';
```

---

## Kafka-топики

| Топик | Партиции | Описание |
|---|---|---|
| `calls-ready` | 3 | MP3 сохранён, готов к транскрибации |
| `transcriptions-done` | 3 | Транскрипция завершена, готово к анализу |
| `analysis-done` | 3 | Анализ завершён (для внешних подписчиков) |
| `calls-errors` | 1 | Ошибки на любом этапе |

### Структура события `calls-ready`

```json
{
  "call_id": "mango_rec123abc",
  "file_path": "calls/2024-07-15/mango_rec123abc.mp3",
  "source": "mango",
  "recording_id": "rec123abc",
  "employee_id": "42",
  "employee_name": "Иван Петров",
  "phone_from": "+79001234567",
  "phone_to": "+74951234567",
  "direction": "inbound",
  "duration": 245,
  "start_time": "2024-07-15T10:28:00Z",
  "size_bytes": 1048576,
  "uploaded_at": "2024-07-15T10:30:00Z"
}
```

---

## Управление и обслуживание

### Ежедневные команды

```bash
# Статус всех контейнеров
docker compose ps

# Логи всех сервисов (live)
docker compose logs -f

# Логи конкретного сервиса
docker compose logs -f mango-collector
docker compose logs -f transcriber
docker compose logs -f analyzer
docker compose logs -f api

# Последние 100 строк логов
docker compose logs --tail=100 analyzer
```

### Перезапуск и обновление

```bash
# Перезапустить один сервис
docker compose restart analyzer

# Пересобрать и перезапустить после изменений кода
docker compose build analyzer
docker compose up -d analyzer

# Пересобрать все сервисы приложения
docker compose build api mango-collector transcriber analyzer collector
docker compose up -d

# Полная остановка
docker compose down

# Остановка с удалением всех данных (ОСТОРОЖНО: удалит базу, MinIO, модели)
docker compose down -v
```

### Резервное копирование

```bash
# Бэкап базы данных
docker compose exec postgres pg_dump -U callsuser callsdb > backup_$(date +%Y%m%d).sql

# Восстановить бэкап
docker compose exec -T postgres psql -U callsuser callsdb < backup_20240715.sql

# MinIO данные хранятся в Docker volume minio_data
# Для бэкапа файлов используй mc (MinIO Client):
docker run --rm -it --network calls_default \
  quay.io/minio/mc \
  mirror local/call-audios /backup/
```

### Масштабирование транскрайбера (если очередь растёт)

```bash
# Запустить 2 экземпляра transcriber
docker compose up -d --scale transcriber=2

# Учти: каждый экземпляр загружает Whisper модель (~1.5 GB RAM)
```

---

## Диагностика и устранение неисправностей

### Проверить все сервисы одной командой

```bash
docker compose ps
# Если сервис не healthy — смотреть логи:
docker compose logs <имя-сервиса>
```

### Kafka не готова / сервисы не запускаются

```bash
# Смотреть логи Kafka
docker compose logs kafka

# Принудительно пересоздать топики
docker compose restart kafka-init

# Список существующих топиков
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Ошибки при транскрибации

```bash
# Смотреть логи транскрайбера
docker compose logs transcriber

# Часто причины:
# 1. Не хватает RAM — уменьши WHISPER_MODEL в .env (например: small вместо medium)
# 2. Звонок уже обработан — проверить статус: GET /calls/{id}/status
# 3. Файл поврежден — проверить в MinIO консоли: http://localhost:9001
```

### Ошибки при анализе (Ollama)

```bash
# Смотреть логи analyzer
docker compose logs analyzer

# Проверить статус Ollama
curl http://localhost:11434/api/tags

# Модель не скачалась — скачать вручную
docker compose exec ollama ollama pull gemma3:4b

# Не хватает RAM — сменить модель в .env
# OLLAMA_MODEL=gemma3:4b  (наименее требовательная)
```

### Проверить очередь в Kafka

```bash
# Сколько необработанных сообщений в calls-ready
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group transcriber-group \
  --describe

# Посмотреть последние сообщения в топике
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic calls-errors \
  --from-beginning \
  --max-messages 10
```

### Ошибки подключения к МангоОфис

```bash
# Проверить логи mango-collector
docker compose logs mango-collector

# Убедиться что API ключи правильные в .env
# Проверить connectivity
docker compose exec mango-collector curl -I https://app.mango-office.ru/vpbx
```

### API возвращает 403 Forbidden

```bash
# Убедиться что заголовок передаётся правильно
curl http://localhost/calls -H "X-API-Key: ВАШ_КЛЮЧ"

# Проверить что API_KEY задан в .env и не пустой
# Если API_KEY не задан в .env, авторизация отключена (dev-режим)
```

---

## Структура проекта

```
calls/
├── .env.example              # Шаблон переменных окружения
├── .gitignore
├── docker-compose.yml        # Описание всех сервисов
├── README.md                 # Этот файл
├── FRONTEND_INTEGRATION.md   # Руководство для команды фронтенда
│
├── db/
│   └── init.sql              # Схема PostgreSQL (автозапуск при старте)
│
├── init-scripts/
│   └── create-topics.sh      # Создание Kafka-топиков
│
├── nginx/
│   └── nginx.conf            # Конфиг nginx (с местом для фронтенда)
│
├── api/                      # Главный REST API
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── mango-collector/          # Автосборщик записей из МангоОфис
│   ├── Dockerfile
│   ├── main.py
│   ├── db.py
│   └── requirements.txt
│
├── collector/                # DEPRECATED: ручная загрузка (замещена /calls/upload)
│   ├── Dockerfile
│   ├── main.py
│   ├── db.py
│   └── requirements.txt
│
├── transcriber/              # Whisper транскрибация
│   ├── Dockerfile
│   ├── main.py
│   ├── audio_processing.py   # Предобработка аудио через PyAV
│   ├── transcription.py      # Обёртка над WhisperModel
│   ├── db.py
│   └── requirements.txt
│
└── analyzer/                 # Ollama/Gemma анализ
    ├── Dockerfile
    ├── main.py
    ├── db.py
    └── requirements.txt
```

---

## Разработка

### Локальная разработка (без Docker)

```bash
# Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установить зависимости нужного сервиса
pip install -r api/requirements.txt

# Запустить только инфраструктуру
docker compose up -d postgres kafka minio zookeeper ollama

# Запустить API локально
POSTGRES_DSN="postgresql://callsuser:callspassword@localhost:5432/callsdb" \
KAFKA_BOOTSTRAP="localhost:9092" \
MINIO_ENDPOINT="http://localhost:9000" \
API_KEY="dev-key" \
uvicorn api.main:app --reload --port 8000
```

### Переменные окружения для dev-режима

Если `API_KEY` не задан в `.env` — авторизация отключена, можно обращаться к API без заголовка.

### Добавить новую модель Ollama

```bash
# Войти в контейнер Ollama
docker compose exec ollama ollama pull llama3:8b

# Изменить OLLAMA_MODEL в .env
# Перезапустить analyzer
docker compose restart analyzer
```

### Продолжение разработки на другом компьютере

1. Склонировать репозиторий с GitHub
2. Скопировать `.env` файл (не коммитится в git)
3. Выполнить `docker compose up -d`
4. База данных и MinIO данные создадутся заново (данные не переносятся)

Для переноса данных:
```bash
# На старом ПК
docker compose exec postgres pg_dump -U callsuser callsdb > db_backup.sql
docker run --rm -v calls_minio_data:/data -v $(pwd):/backup alpine \
  tar czf /backup/minio_backup.tar.gz /data

# На новом ПК (после запуска docker compose up -d postgres minio)
docker compose exec -T postgres psql -U callsuser callsdb < db_backup.sql
docker run --rm -v calls_minio_data:/data -v $(pwd):/backup alpine \
  tar xzf /backup/minio_backup.tar.gz -C /
```

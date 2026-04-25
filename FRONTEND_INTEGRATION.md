# Руководство по интеграции фронтенда

Этот документ предназначен для команды фронтенда. Здесь описано как подключить фронтенд к работающей системе: конфигурация Docker Compose, nginx, и полная документация API.

## Содержание

- [Подключение фронтенда к Docker Compose](#подключение-фронтенда-к-docker-compose)
- [Настройка nginx](#настройка-nginx)
- [Авторизация в API](#авторизация-в-api)
- [Справочник по API](#справочник-по-api)
- [Примеры на JavaScript/TypeScript](#примеры-на-javascripttypescript)
- [Рекомендации по UX](#рекомендации-по-ux)

---

## Подключение фронтенда к Docker Compose

### Вариант 1 — Фронтенд как отдельный Docker-сервис (рекомендуется)

Добавь в `docker-compose.yml` новый сервис перед блоком `nginx`:

```yaml
  frontend:
    build: ./frontend          # папка с вашим проектом (Dockerfile должен быть там)
    restart: unless-stopped
    environment:
      VITE_API_BASE_URL: ""    # пустая строка = запросы идут на тот же хост через nginx
    # Не выставляй порт наружу — nginx проксирует трафик

  nginx:
    # ... (существующая конфигурация) ...
    depends_on:
      api:
        condition: service_healthy
      frontend:               # добавить зависимость
        condition: service_started
```

Пример `frontend/Dockerfile` для Vite/React:
```dockerfile
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx-frontend.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

### Вариант 2 — Собранная статика через volume

Если вы собираете фронтенд вне Docker и передаёте только `dist/`:

1. Добавь в `docker-compose.yml` к сервису `nginx`:
```yaml
  nginx:
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./frontend/dist:/usr/share/nginx/html:ro    # добавить эту строку
```

2. Пересобирай `dist/` локально командой `npm run build` и делай `docker compose restart nginx`.

---

## Настройка nginx

Открой файл `nginx/nginx.conf`. Найди блок с комментарием `МЕСТО ДЛЯ ФРОНТЕНДА` в конце файла и замени его.

### Для Варианта 1 (проксирование на frontend-сервис)

```nginx
# В начале файла добавь upstream:
upstream frontend_backend {
    server frontend:80;    # имя сервиса из docker-compose
}

# Замени блок location / { ... } на:
location / {
    proxy_pass         http://frontend_backend;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;

    # Для SPA (React/Vue/Angular) — fallback на index.html при 404
    proxy_intercept_errors on;
    error_page 404 = @fallback;
}

location @fallback {
    proxy_pass http://frontend_backend/index.html;
}
```

### Для Варианта 2 (статика из volume)

```nginx
location / {
    root  /usr/share/nginx/html;
    index index.html;
    try_files $uri $uri/ /index.html;   # SPA-режим: все пути → index.html
}
```

### Применить изменения

```bash
# Проверить конфиг без перезапуска
docker compose exec nginx nginx -t

# Применить
docker compose restart nginx
# или (без прерывания соединений)
docker compose exec nginx nginx -s reload
```

---

## Авторизация в API

Все запросы (кроме `/health`) требуют заголовок:

```
X-API-Key: <значение API_KEY из файла .env>
```

Ключ выдаёт команда бэкенда/DevOps. Храни его в переменных окружения фронтенда, не в коде.

Пример для `.env.local` (Vite):
```
VITE_API_KEY=сюда_вставить_ключ
```

Доступ в коде:
```typescript
const API_KEY = import.meta.env.VITE_API_KEY;
```

**Важно:** Заголовок `X-API-Key` настроен в nginx и проксируется на бэкенд. Если фронтенд отдаёт nginx, а API-запросы идут на тот же хост — ключ передаётся напрямую от браузера к API. Это нормально для внутренней системы.

---

## Справочник по API

Базовый URL: `http://localhost` (в продакшне замени на реальный хост)

Интерактивная документация Swagger: `http://localhost/docs`

---

### GET /health

Проверка доступности системы. **Без авторизации.**

**Ответ:**
```json
{"status": "ok", "service": "api"}
```

Используй для проверки доступности перед показом UI.

---

### POST /calls/upload

Ручная загрузка MP3-файла.

**Заголовки:**
```
X-API-Key: <ключ>
Content-Type: multipart/form-data
```

**Поля формы:**
| Поле | Тип | Обязательно |
|---|---|---|
| `file` | File (MP3) | да |
| `employee_id` | string | нет |
| `employee_name` | string | нет |
| `phone_from` | string | нет |
| `phone_to` | string | нет |
| `direction` | `inbound` / `outbound` | нет |

**Ответ 200:**
```json
{
  "status": "uploaded",
  "call_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_path": "calls/2024-07-15/550e8400-e29b-41d4-a716-446655440000.mp3"
}
```

**Ошибки:**
- `400` — не MP3 или пустой файл
- `403` — неверный API-ключ

---

### GET /calls

Список звонков с фильтрами и пагинацией.

**Заголовки:** `X-API-Key: <ключ>`

**Query-параметры:**
| Параметр | Тип | Описание |
|---|---|---|
| `status` | string | `stored`, `transcribed`, `analyzed`, `failed_*` |
| `source` | string | `mango` или `manual` |
| `employee_id` | string | ID сотрудника |
| `date_from` | string | Дата в формате `YYYY-MM-DD` |
| `date_to` | string | Дата в формате `YYYY-MM-DD` |
| `limit` | int | 1–500, по умолчанию 50 |
| `offset` | int | Для пагинации, по умолчанию 0 |

**Ответ 200:**
```json
{
  "total": 142,
  "limit": 50,
  "offset": 0,
  "calls": [
    {
      "call_id": "mango_abc123",
      "mango_recording_id": "abc123",
      "source": "mango",
      "employee_id": "42",
      "employee_name": "Иван Петров",
      "phone_from": "+79001234567",
      "phone_to": "+74951234567",
      "direction": "inbound",
      "duration_seconds": 245,
      "start_time": "2024-07-15T10:28:00Z",
      "file_size_bytes": 1048576,
      "status": "analyzed",
      "transcribed_at": "2024-07-15T10:35:00Z",
      "analyzed_at": "2024-07-15T10:38:00Z",
      "analysis_sentiment": "positive",
      "analysis_score": 8,
      "analysis_call_outcome": "sold",
      "created_at": "2024-07-15T10:30:00Z"
    }
  ]
}
```

---

### GET /calls/{call_id}

Полная информация о звонке (все поля включая транскрипцию и анализ).

**Заголовки:** `X-API-Key: <ключ>`

`call_id` — внутренний UUID или ID записи МангоОфис.

**Ответ 200:** полная строка таблицы `calls` (все поля из схемы БД)

**Ошибки:**
- `404` — звонок не найден

---

### GET /calls/{call_id}/status

Только статус и временные метки (лёгкий запрос для polling).

**Заголовки:** `X-API-Key: <ключ>`

**Ответ 200:**
```json
{
  "call_id": "mango_abc123",
  "mango_recording_id": "abc123",
  "status": "analyzed",
  "created_at": "2024-07-15T10:30:00Z",
  "transcribed_at": "2024-07-15T10:35:20Z",
  "analyzed_at": "2024-07-15T10:38:45Z"
}
```

**Жизненный цикл статуса:**
```
pending → stored → transcribed → analyzed
              ↓          ↓             ↓
      failed_download  failed_    failed_
                      transcription  analysis
```

---

### GET /calls/{call_id}/audio

Скачать MP3-файл (стриминг).

**Заголовки:** `X-API-Key: <ключ>`

**Ответ:** binary stream `audio/mpeg`

Используй для встроенного аудиоплеера (см. примеры ниже).

**Ошибки:**
- `404` — файл не найден (статус ещё `pending` или ошибка загрузки)

---

### GET /calls/{call_id}/transcript

Транскрипция с временными метками сегментов.

**Заголовки:** `X-API-Key: <ключ>`

**Ответ 200:**
```json
{
  "call_id": "mango_abc123",
  "language": "ru",
  "language_probability": 0.99,
  "full_text": "Добрый день, компания Апогей...",
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

**Ошибки:**
- `404` — транскрипция ещё не готова (ответ содержит текущий `status`)

---

### GET /calls/{call_id}/analysis

Результаты AI-анализа.

**Заголовки:** `X-API-Key: <ключ>`

**Ответ 200:**
```json
{
  "call_id": "mango_abc123",
  "summary": "Клиент обратился за консультацией по тарифам...",
  "sentiment": "positive",
  "script_compliance": true,
  "script_violations": [],
  "action_items": [
    "Перезвонить клиенту в среду",
    "Отправить КП на email"
  ],
  "score": 8,
  "topics": ["тарифы", "подключение"],
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

Справочник по значениям:
- `sentiment`: `positive` | `neutral` | `negative`
- `script_compliance`: `true` (соответствует скрипту) | `false` (есть нарушения)
- `score`: 1–10
- `call_outcome`: `sold` | `callback` | `rejected` | `escalated` | `consultation` | `other`

**Ошибки:**
- `404` — анализ ещё не готов

---

### GET /admin/stats

Агрегированная статистика для дашборда.

**Заголовки:** `X-API-Key: <ключ>`

**Ответ 200:**
```json
{
  "generated_at": "2024-07-15T12:00:00Z",
  "calls": {
    "total": 1542,
    "by_status": {
      "analyzed": 1480,
      "transcribed": 12,
      "stored": 8,
      "failed_analysis": 30
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

### GET /admin/logs

Журнал операций и ошибок.

**Заголовки:** `X-API-Key: <ключ>`

**Query-параметры:**
| Параметр | Описание |
|---|---|
| `level` | `info` / `warning` / `error` |
| `stage` | `download` / `upload` / `transcription` / `analysis` |
| `call_id` | Логи конкретного звонка |
| `limit` | 1–1000, по умолчанию 100 |
| `offset` | Для пагинации |

**Ответ 200:**
```json
{
  "total": 8,
  "limit": 100,
  "offset": 0,
  "logs": [
    {
      "id": 1042,
      "call_id": "mango_abc123",
      "level": "info",
      "stage": "download",
      "message": "Запись скачана и загружена (1048576 байт)",
      "details": null,
      "created_at": "2024-07-15T10:30:05Z"
    }
  ]
}
```

---

## Примеры на JavaScript/TypeScript

### Базовый API-клиент

```typescript
const API_BASE = "";          // пустая строка = тот же хост через nginx
const API_KEY = import.meta.env.VITE_API_KEY ?? "";

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "X-API-Key": API_KEY,
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail ?? `HTTP ${res.status}`);
  }

  return res.json();
}
```

### Список звонков с пагинацией

```typescript
interface Call {
  call_id: string;
  employee_name: string | null;
  phone_from: string | null;
  phone_to: string | null;
  direction: string;
  duration_seconds: number | null;
  status: string;
  analysis_sentiment: string | null;
  analysis_score: number | null;
  analysis_call_outcome: string | null;
  created_at: string;
}

interface CallsResponse {
  total: number;
  limit: number;
  offset: number;
  calls: Call[];
}

async function getCalls(params: {
  status?: string;
  source?: string;
  employee_id?: string;
  date_from?: string;
  date_to?: string;
  limit?: number;
  offset?: number;
}): Promise<CallsResponse> {
  const q = new URLSearchParams(
    Object.fromEntries(
      Object.entries(params)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => [k, String(v)])
    )
  );
  return apiFetch<CallsResponse>(`/calls?${q}`);
}

// Использование:
const { total, calls } = await getCalls({ status: "analyzed", limit: 20, offset: 0 });
```

### Статус конкретного звонка

```typescript
interface CallStatus {
  call_id: string;
  status: string;
  created_at: string;
  transcribed_at: string | null;
  analyzed_at: string | null;
}

async function getCallStatus(callId: string): Promise<CallStatus> {
  return apiFetch<CallStatus>(`/calls/${callId}/status`);
}
```

### Polling статуса до завершения

```typescript
async function waitForAnalysis(callId: string, timeoutMs = 600_000): Promise<CallStatus> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const status = await getCallStatus(callId);

    if (status.status === "analyzed") return status;
    if (status.status.startsWith("failed_")) {
      throw new Error(`Обработка завершилась ошибкой: ${status.status}`);
    }

    // Ожидать 5 секунд перед следующей проверкой
    await new Promise((r) => setTimeout(r, 5000));
  }

  throw new Error("Timeout: анализ не завершился за отведённое время");
}
```

### Встроенный аудиоплеер

```typescript
function getAudioUrl(callId: string): string {
  // Браузер не умеет передавать заголовки в <audio src="...">
  // Поэтому получаем blob через fetch и создаём object URL
  return `/calls/${callId}/audio`;
}

async function loadAudioBlob(callId: string): Promise<string> {
  const res = await fetch(`/calls/${callId}/audio`, {
    headers: { "X-API-Key": API_KEY },
  });
  if (!res.ok) throw new Error(`Аудио недоступно: ${res.status}`);
  const blob = await res.blob();
  return URL.createObjectURL(blob);   // не забудь вызвать URL.revokeObjectURL() при размонтировании
}

// React пример:
// const [audioUrl, setAudioUrl] = useState<string>();
// useEffect(() => {
//   loadAudioBlob(callId).then(setAudioUrl);
//   return () => audioUrl && URL.revokeObjectURL(audioUrl);
// }, [callId]);
// <audio controls src={audioUrl} />
```

### Транскрипция и анализ

```typescript
interface Transcript {
  call_id: string;
  language: string;
  full_text: string;
  segments: Array<{ id: number; start: number; end: number; text: string }>;
  transcribed_at: string;
}

interface Analysis {
  call_id: string;
  summary: string;
  sentiment: "positive" | "neutral" | "negative";
  script_compliance: boolean;
  script_violations: string[];
  action_items: string[];
  score: number;
  topics: string[];
  call_outcome: string;
  analyzed_at: string;
  metadata: {
    employee_id: string | null;
    employee_name: string | null;
    duration_seconds: number | null;
    direction: string | null;
  };
}

const getTranscript = (callId: string) =>
  apiFetch<Transcript>(`/calls/${callId}/transcript`);

const getAnalysis = (callId: string) =>
  apiFetch<Analysis>(`/calls/${callId}/analysis`);
```

### Загрузка файла (ручной upload)

```typescript
async function uploadCall(file: File, meta: {
  employee_id?: string;
  employee_name?: string;
  phone_from?: string;
  phone_to?: string;
  direction?: "inbound" | "outbound";
}): Promise<{ call_id: string; file_path: string }> {
  const form = new FormData();
  form.append("file", file);
  if (meta.employee_id) form.append("employee_id", meta.employee_id);
  if (meta.employee_name) form.append("employee_name", meta.employee_name);
  if (meta.phone_from) form.append("phone_from", meta.phone_from);
  if (meta.phone_to) form.append("phone_to", meta.phone_to);
  if (meta.direction) form.append("direction", meta.direction);

  const res = await fetch("/calls/upload", {
    method: "POST",
    headers: { "X-API-Key": API_KEY },   // НЕ ставь Content-Type вручную — браузер ставит с boundary автоматически
    body: form,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail ?? `Upload failed: HTTP ${res.status}`);
  }

  return res.json();
}
```

### Статистика для дашборда

```typescript
interface Stats {
  generated_at: string;
  calls: {
    total: number;
    by_status: Record<string, number>;
    last_24h: { downloaded: number; transcribed: number; analyzed: number };
  };
  errors: { last_24h: number; last_7d: number };
  storage: { total_size_bytes: number; total_size_mb: number };
  last_activity: {
    last_download: string | null;
    last_transcription: string | null;
    last_analysis: string | null;
    last_error: string | null;
  };
}

const getStats = () => apiFetch<Stats>("/admin/stats");
```

---

## Рекомендации по UX

### Отображение статуса обработки

```
pending           → "Скачивается..."          (spinner)
stored            → "В очереди на расшифровку" (spinner)
transcribed       → "Анализируется..."         (spinner)
analyzed          → "Готово"                   (checkmark)
failed_download   → "Ошибка загрузки"          (warning, retry кнопка)
failed_transcription → "Ошибка расшифровки"   (warning)
failed_analysis   → "Ошибка анализа"           (warning)
```

### Время обработки (ориентировочно, CPU-сервер)

| Этап | Время для 5-минутного звонка |
|---|---|
| Скачивание из МангоОфис | 5–30 сек |
| Транскрибация (Whisper medium) | 1–3 мин |
| Анализ (Gemma 4b) | 30–90 сек |
| **Итого** | **2–5 мин** |

Рекомендуемый интервал polling для статуса: **5–10 секунд**

### Отображение тональности

| Значение | Рекомендуемый цвет | Иконка |
|---|---|---|
| `positive` | зелёный | 😊 или ✓ |
| `neutral` | серый / синий | 😐 или — |
| `negative` | красный / оранжевый | 😞 или ✗ |

### Отображение итога звонка

| `call_outcome` | Отображение |
|---|---|
| `sold` | Продажа |
| `callback` | Перезвон |
| `rejected` | Отказ |
| `escalated` | Эскалация |
| `consultation` | Консультация |
| `other` | Другое |

### Пагинация

API возвращает `total` — используй его для построения пагинации:

```typescript
const totalPages = Math.ceil(total / limit);
const currentPage = Math.floor(offset / limit) + 1;
```

### Дашборд — рекомендуемые виджеты

На основе данных из `GET /admin/stats`:

1. **Счётчики сегодня:** загружено / расшифровано / проанализировано (`last_24h`)
2. **Воронка обработки:** `by_status` как горизонтальный прогресс-бар или диаграмма
3. **Ошибки:** `errors.last_24h` и `errors.last_7d` — с ссылкой на журнал
4. **Хранилище:** `storage.total_size_mb` — с прогресс-баром если есть лимит
5. **Последняя активность:** `last_activity.*` — показать `X минут назад`

### Журнал ошибок

Используй `GET /admin/logs?level=error` для страницы журнала ошибок. Полезные фильтры для UI:
- По типу ошибки (`stage`)
- По времени (через `date_from` / `date_to` на уровне `/calls`, для логов — фильтруй на фронте)
- По звонку (передай `call_id` для детального просмотра конкретного звонка)

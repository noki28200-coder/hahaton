-- ============================================================
-- Схема базы данных: Система обработки записей звонков
-- PostgreSQL 16+
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Основная таблица звонков ─────────────────────────────────
CREATE TABLE IF NOT EXISTS calls (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Идентификаторы
    call_id             VARCHAR(255) UNIQUE NOT NULL,       -- внутренний ID
    mango_recording_id  VARCHAR(255) UNIQUE,                -- ID записи в МангоОфис (осн. идентификатор)
    source              VARCHAR(50)  NOT NULL DEFAULT 'manual', -- 'mango' | 'manual'

    -- Метаданные звонка
    employee_id         VARCHAR(255),
    employee_name       VARCHAR(500),
    phone_from          VARCHAR(50),
    phone_to            VARCHAR(50),
    direction           VARCHAR(20),                        -- 'inbound' | 'outbound'
    duration_seconds    INTEGER,
    start_time          TIMESTAMPTZ,

    -- Аудиофайл (MinIO)
    file_path           VARCHAR(1000),                      -- ключ объекта в MinIO
    file_size_bytes     BIGINT,

    -- Статус обработки
    -- pending → stored → transcribed → analyzed
    -- failed_download | failed_transcription | failed_analysis
    status              VARCHAR(50)  NOT NULL DEFAULT 'pending',

    -- Транскрипция (Whisper)
    transcript_text                 TEXT,
    transcript_language             VARCHAR(10),
    transcript_language_probability REAL,
    transcript_segments             JSONB,                  -- массив [{start,end,text}]
    transcribed_at                  TIMESTAMPTZ,

    -- Анализ (Ollama / Gemma)
    analysis_summary            TEXT,
    analysis_sentiment          VARCHAR(20),                -- positive | neutral | negative
    analysis_script_compliance  BOOLEAN,
    analysis_script_violations  JSONB,                      -- массив строк
    analysis_action_items       JSONB,                      -- массив строк
    analysis_score              SMALLINT,                   -- 1..10
    analysis_topics             JSONB,                      -- массив строк
    analysis_call_outcome       VARCHAR(50),                -- sold|callback|rejected|escalated|consultation|other

    analyzed_at         TIMESTAMPTZ,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы для частых запросов
CREATE INDEX IF NOT EXISTS idx_calls_status           ON calls(status);
CREATE INDEX IF NOT EXISTS idx_calls_created_at       ON calls(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_employee_id      ON calls(employee_id);
CREATE INDEX IF NOT EXISTS idx_calls_source           ON calls(source);
CREATE INDEX IF NOT EXISTS idx_calls_mango_id         ON calls(mango_recording_id);
CREATE INDEX IF NOT EXISTS idx_calls_phone_from       ON calls(phone_from);
CREATE INDEX IF NOT EXISTS idx_calls_phone_to         ON calls(phone_to);
CREATE INDEX IF NOT EXISTS idx_calls_start_time       ON calls(start_time DESC);

-- ── Журнал операций и ошибок ─────────────────────────────────
CREATE TABLE IF NOT EXISTS call_logs (
    id          BIGSERIAL    PRIMARY KEY,
    call_id     VARCHAR(255),                               -- связь с calls.call_id (не FK, мягкая)
    level       VARCHAR(20)  NOT NULL DEFAULT 'info',       -- info | warning | error
    stage       VARCHAR(50),                                -- download | upload | transcription | analysis
    message     TEXT         NOT NULL,
    details     JSONB,                                      -- произвольные детали
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_call_logs_call_id    ON call_logs(call_id);
CREATE INDEX IF NOT EXISTS idx_call_logs_created_at ON call_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_call_logs_level      ON call_logs(level);
CREATE INDEX IF NOT EXISTS idx_call_logs_stage      ON call_logs(stage);

-- ── Авто-обновление updated_at ───────────────────────────────
CREATE OR REPLACE FUNCTION _fn_update_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_calls_updated_at ON calls;
CREATE TRIGGER trg_calls_updated_at
    BEFORE UPDATE ON calls
    FOR EACH ROW EXECUTE FUNCTION _fn_update_updated_at();

-- ── Начальная запись в лог ───────────────────────────────────
INSERT INTO call_logs (level, stage, message)
VALUES ('info', 'system', 'База данных инициализирована')
ON CONFLICT DO NOTHING;

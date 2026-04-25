"""
HTTP-сервис транскрибации (WhisperX HTTP API).

Принимает аудио по HTTP, возвращает транскрипцию.
Вся оркестрация (Kafka, MinIO, PostgreSQL) вынесена в transcriber-go.

POST /transcribe
  multipart/form-data:
    file: аудиофайл (MP3, WAV, ...)
    language: код языка (default: ru)
  → JSON: {language, language_probability, segments, full_text}

GET /health → {"status": "ok"}
"""

import logging
import os
from contextlib import asynccontextmanager
from io import BytesIO

import av
import numpy as np
import uvicorn
from faster_whisper import WhisperModel
from fastapi import FastAPI, File, Form, HTTPException, UploadFile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [whisperx] %(levelname)s: %(message)s",
)
logger = logging.getLogger("whisperx")

WHISPER_MODEL_NAME = os.getenv("WHISPER_MODEL", "medium")
WHISPER_THREADS = int(os.getenv("WHISPER_THREADS", "4"))
DEFAULT_LANGUAGE = os.getenv("WHISPER_LANGUAGE", "ru")

_model: WhisperModel = None


def _prepare_audio(data: bytes) -> np.ndarray:
    """Декодировать аудио → 16kHz моно float32."""
    container = av.open(BytesIO(data))
    stream = container.streams.audio[0]

    graph = av.filter.Graph()
    source = graph.add_abuffer(
        sample_rate=stream.rate,
        format=stream.format.name,
        layout=stream.layout.name if stream.layout else None,
        time_base=stream.time_base,
    )
    hpass = graph.add("highpass", f="200")
    resample = graph.add("aresample", "16000")
    sink = graph.add("abuffersink")

    source.link_to(hpass)
    hpass.link_to(resample)
    resample.link_to(sink)
    graph.configure()

    frames = []
    for frame in container.decode(audio=0):
        graph.push(frame)
        while True:
            try:
                ff = graph.pull()
            except (av.error.BlockingIOError, BlockingIOError, av.EOFError):
                break
            arr = ff.to_ndarray()
            if arr.ndim > 1:
                arr = arr.mean(axis=0)
            frames.append(arr.astype(np.float32))

    graph.push(None)
    while True:
        try:
            ff = graph.pull()
        except (av.error.BlockingIOError, BlockingIOError, av.EOFError):
            break
        arr = ff.to_ndarray()
        if arr.ndim > 1:
            arr = arr.mean(axis=0)
        frames.append(arr.astype(np.float32))

    container.close()

    audio = np.concatenate(frames) if frames else np.array([], dtype=np.float32)
    mx = np.max(np.abs(audio)) if audio.size else 0
    if mx > 0:
        audio = audio / mx
    return audio


def _transcribe(audio: np.ndarray, language: str) -> dict:
    segments_iter, info = _model.transcribe(
        audio,
        beam_size=2,
        language=language,
        vad_filter=True,
        vad_parameters=dict(min_silence_duration_ms=1000, speech_pad_ms=400),
        initial_prompt="Разговор по телефону сотрудника и клиента, речь чёткая",
    )

    segments, parts = [], []
    for seg in segments_iter:
        text = seg.text.strip()
        if len(text) <= 1:
            continue
        segments.append({"start": round(seg.start, 3), "end": round(seg.end, 3), "text": text})
        parts.append(text)

    return {
        "language": info.language,
        "language_probability": round(info.language_probability, 3),
        "segments": segments,
        "full_text": " ".join(parts),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _model
    logger.info("Загружаю Whisper модель '%s'...", WHISPER_MODEL_NAME)
    _model = WhisperModel(
        WHISPER_MODEL_NAME,
        device="cpu",
        compute_type="int8",
        cpu_threads=WHISPER_THREADS,
        num_workers=1,
    )
    logger.info("Whisper готов.")
    yield


app = FastAPI(title="WhisperX Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "model": WHISPER_MODEL_NAME}


@app.post("/transcribe")
async def transcribe(
    file: UploadFile = File(...),
    language: str = Form(default=""),
):
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Файл пустой")

    lang = language or DEFAULT_LANGUAGE

    try:
        audio = _prepare_audio(data)
    except Exception as exc:
        logger.error("Ошибка декодирования аудио: %s", exc)
        raise HTTPException(status_code=422, detail=f"Ошибка декодирования аудио: {exc}")

    try:
        result = _transcribe(audio, lang)
    except Exception as exc:
        logger.error("Ошибка транскрибации: %s", exc)
        raise HTTPException(status_code=500, detail=f"Ошибка транскрибации: {exc}")

    logger.info("Транскрибировано: %d сег., lang=%s (p=%.2f)",
                len(result["segments"]), result["language"], result["language_probability"])
    return result


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")

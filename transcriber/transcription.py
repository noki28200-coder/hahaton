"""
Утилита транскрибации: принимает готовую модель и аудио-массив,
возвращает структурированный результат.
"""

import numpy as np
from faster_whisper import WhisperModel


def transcribe_audio(model: WhisperModel, audio_data: np.ndarray, language: str = "ru") -> dict:
    """
    Транскрибирует аудио и возвращает словарь с сегментами и полным текстом.

    Args:
        model:      загруженная WhisperModel (инициализируется один раз при старте)
        audio_data: float32 numpy массив, 16kHz, моно
        language:   код языка (ru, en, ...)

    Returns:
        {
            "language": "ru",
            "language_probability": 0.98,
            "segments": [{"start": 0.0, "end": 2.5, "text": "..."}],
            "full_text": "полный текст звонка"
        }
    """
    segments_iter, info = model.transcribe(
        audio_data,
        beam_size=2,
        language=language,
        vad_filter=True,
        vad_parameters=dict(
            min_silence_duration_ms=1000,
            speech_pad_ms=400,
        ),
        initial_prompt="Разговор по телефону сотрудника и клиента, речь чёткая",
    )

    segments = []
    text_parts = []

    for seg in segments_iter:
        text = seg.text.strip()
        if len(text) <= 1:
            continue
        segments.append({
            "start": round(seg.start, 3),
            "end": round(seg.end, 3),
            "text": text,
        })
        text_parts.append(text)

    return {
        "language": info.language,
        "language_probability": round(info.language_probability, 3),
        "segments": segments,
        "full_text": " ".join(text_parts),
    }

import av
import numpy as np
from io import BytesIO


def prepare_audio_for_whisper_bytes(data: bytes) -> np.ndarray:
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

    audio_frames = []

    for frame in container.decode(audio=0):
        graph.push(frame)
        while True:
            try:
                filtered_frame = graph.pull()
            except (av.error.BlockingIOError, BlockingIOError):
                break
            except av.EOFError:
                break

            data = filtered_frame.to_ndarray()
            if data.ndim > 1:
                data = data.mean(axis=0)
            audio_frames.append(data.astype(np.float32))

    graph.push(None)
    while True:
        try:
            filtered_frame = graph.pull()
        except (av.error.BlockingIOError, BlockingIOError, av.EOFError):
            break

        data = filtered_frame.to_ndarray()
        if data.ndim > 1:
            data = data.mean(axis=0)
        audio_frames.append(data.astype(np.float32))

    container.close()

    audio_data = np.concatenate(audio_frames) if audio_frames else np.array([], dtype=np.float32)
    max_val = np.max(np.abs(audio_data)) if audio_data.size else 0
    if max_val > 0:
        audio_data = audio_data / max_val

    return audio_data
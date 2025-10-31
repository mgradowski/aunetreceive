import asyncio
from collections.abc import AsyncIterator
from random import random
from typing import Callable, Never

import alsaaudio

_PERIODSIZE = 64
_PERIODS = 96
_AVAIL_TARGET = (_PERIODSIZE * _PERIODS) // 2
FRAMESIZE = 4


async def _unpack_frames(
    queue: asyncio.Queue[bytes], framesize: int
) -> AsyncIterator[bytes]:
    """Unpacks AUNetSend chunks (1024 bytes) into frames."""
    while True:
        chunk = await queue.get()
        for i in range(len(chunk) // framesize):
            yield chunk[i * framesize : (i + 1) * framesize]


async def _accordion(
    frames: AsyncIterator[bytes],
    getspeed: Callable[[], float],
) -> AsyncIterator[bytes]:
    """Call getspeed() for each frame. Duplicate each frame (yield it twice)
    with probability abs(getspeed()) when getspeed() < 0.0, skip each frame
    with probability abs(getspeed()) when getspeed() > 0.0. Otherwise, when
    getspeed() is 0.0, simply yield it."""

    async for frame in frames:
        speed = getspeed()
        if speed < 0.0 and random() < abs(speed):
            # underrun <-> slow down audio
            yield frame
            yield frame
        elif speed > 0.0 and random() < abs(speed):
            # overrun <-> speed up audio
            pass
        else:
            yield frame


async def _pack_frames(
    frames: AsyncIterator[bytes], periodsize: int
) -> AsyncIterator[bytes]:
    """Join and yield `periodsize` frames into a period for playback."""
    period: list[bytes] = []
    async for frame in frames:
        period.append(frame)
        if len(period) == periodsize:
            yield b"".join(period)
            period = []


async def play_forever(
    chunks: asyncio.Queue[bytes],
    device: alsaaudio.PCM,
    momentum: float = 5e-4,
    sensitivity: float = 1e-5,
) -> Never:
    """Play `chunks` on `device`, while adding and/or removing single frames to maintain
    `(device.avail() - chunks.qsize() * <frames per chunk>) == (<buffer size in frames> // 2)`."""
    speed = 0.0
    getspeed = lambda: speed  # noqa: E731  # pyright: ignore[reportUnknownLambdaType]

    frames_received = _unpack_frames(chunks, FRAMESIZE)
    frames_corrected = _accordion(frames_received, getspeed)
    playback_periods = _pack_frames(frames_corrected, _PERIODSIZE)

    async for period in playback_periods:
        avail = device.avail()
        device.write(period)

        err = avail - chunks.qsize() * (1024 // FRAMESIZE) - _AVAIL_TARGET
        speed = momentum * -err * sensitivity + (1.0 - momentum) * speed

    assert False

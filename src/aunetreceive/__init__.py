import asyncio
import logging
import warnings
from dataclasses import dataclass
from typing import Never

DEFAULT_AUNETSEND_PORT: int = 52800

_log = logging.getLogger(__name__)


@dataclass
class _PreHandshakeConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def handshake(self) -> _PreMetadataConnection:
        async with asyncio.timeout(1.0):
            payload = await self.reader.read(16)

        assert len(payload) == 16
        assert payload.startswith(b"ausend")

        async with asyncio.timeout(1.0):
            self.writer.write(b"aurecv".ljust(40, b"\0"))
            await self.writer.drain()

        return _PreMetadataConnection(self.reader, self.writer)


@dataclass
class _PreMetadataConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def discard_metadata(self) -> _PreSyncConnection:
        async with asyncio.timeout(1.0):
            metadata = await self.reader.read(40)
        assert len(metadata) == 40

        return _PreSyncConnection(self.reader, self.writer)


@dataclass
class _PreSyncConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def sync(self) -> _ReadyConnection:
        async with asyncio.timeout(1.0):
            while await self.reader.read(4) != b"sync":
                continue
        return _ReadyConnection(self.reader, self.writer)


@dataclass
class _ReadyConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    async def get_chunk(self) -> tuple[bytes, _PreSyncConnection]:
        result = await self.reader.read(1024)
        return result, _PreSyncConnection(self.reader, self.writer)


async def receive_forever(
    result_queue: asyncio.Queue[bytes],
    host: str,
    port: int = DEFAULT_AUNETSEND_PORT,
    connect_timeout_sec: float = 5.0,
    put_timeout_sec: float | None = None,
    ignore_overruns: bool = True,
) -> Never:
    assert connect_timeout_sec > 0.0
    assert put_timeout_sec is None or put_timeout_sec > 0.0

    async with asyncio.timeout(connect_timeout_sec):
        reader, writer = await asyncio.open_connection(host, port)

    _log.info("connected to %s:%d", host, port)

    pre_handshake = _PreHandshakeConnection(reader, writer)
    pre_metadata = await pre_handshake.handshake()
    pre_sync = await pre_metadata.discard_metadata()

    while True:
        ready = await pre_sync.sync()
        data, pre_sync = await ready.get_chunk()

        if put_timeout_sec is None and ignore_overruns:
            try:
                result_queue.put_nowait(data)
            except asyncio.QueueFull:
                warnings.warn(f"buffer overrun { result_queue.qsize()= }")
        elif put_timeout_sec is None:
            result_queue.put_nowait(data)
        elif ignore_overruns and ignore_overruns:
            try:
                async with asyncio.timeout(put_timeout_sec):
                    await result_queue.put(data)
            except asyncio.TimeoutError:
                warnings.warn(f"buffer overrun { result_queue.qsize()= }")
        else:
            async with asyncio.timeout(put_timeout_sec):
                await result_queue.put(data)

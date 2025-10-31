import asyncio
import logging
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
) -> Never:
    assert connect_timeout_sec > 0.0

    async with asyncio.timeout(connect_timeout_sec):
        reader, writer = await asyncio.open_connection(host, port)

    _log.info("connected to %s:%d", host, port)

    pre_handshake = _PreHandshakeConnection(reader, writer)
    pre_metadata = await pre_handshake.handshake()
    pre_sync = await pre_metadata.discard_metadata()

    while True:
        ready = await pre_sync.sync()
        data, pre_sync = await ready.get_chunk()

        await result_queue.put(data)

import asyncio
import ipaddress
import logging
import sys
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from typing import TypedDict, cast

from zeroconf.asyncio import AsyncZeroconf

from aunetreceive import (
    DEFAULT_AUNETSEND_PORT,
    receive_forever,
)

BONJOUR_SERVICE_TYPE: str = "_apple-ausend._tcp.local."
DEFAULT_AUNETSEND_BONJOUR_NAME: str = "AUNetSend"

_log = logging.getLogger(__name__)


class _Args(TypedDict):
    outfile: Path | None
    host: str | None
    port: int | None
    bonjour_name: str | None


async def amain() -> None:
    logging.basicConfig(
        stream=sys.stderr,
        format="[{asctime}] [{levelname:.1s}:{name}] {message}",
        style="{",
        level=logging.INFO,
    )
    logging.captureWarnings(True)

    argparser = ArgumentParser("aunetreceive")
    _ = argparser.add_argument(
        "-H",
        "--host",
        type=str,
        help="AUNetSend host",
    )
    _ = argparser.add_argument(
        "-p",
        "--port",
        type=int,
        help="AUNetSend TCP port number",
    )
    _ = argparser.add_argument(
        "-B",
        "--bonjour-name",
        type=str,
    )
    _ = argparser.add_argument("outfile", type=Path, nargs="?")
    args = cast(_Args, cast(object, vars(argparser.parse_args())))

    host: str
    port: int

    if args["bonjour_name"] and (args["host"] or args["port"]):
        print("--host/-H and --port/-p are mutually exclusive with --bonjour-name/-B")
        sys.exit(1)
    elif args["host"] and not args["port"]:
        host = args["host"]
        port = DEFAULT_AUNETSEND_PORT
    elif args["host"] and args["port"]:
        host = args["host"]
        port = args["port"]
    else:
        bonjour_name: str = args.get("bonjour_name") or DEFAULT_AUNETSEND_BONJOUR_NAME
        bonjour_fqn = f"{bonjour_name}.{BONJOUR_SERVICE_TYPE}"
        service_info = await AsyncZeroconf().async_get_service_info(
            BONJOUR_SERVICE_TYPE, bonjour_fqn
        )
        if service_info is None:
            raise RuntimeError(
                f"couldn't resolve {bonjour_fqn}, is the server running?"
            )
        host = str(ipaddress.ip_address(sorted(service_info.addresses)[0]))
        if service_info.port is not None:
            port = service_info.port
            _log.info(f"{bonjour_fqn} is {host}:{port}")
        else:
            _log.info("%s is %s:<unknown port>", bonjour_fqn, host)
            _log.info("using default port %d", DEFAULT_AUNETSEND_PORT)
            port = DEFAULT_AUNETSEND_PORT

    outfile_contextmanager = (
        open(args["outfile"], mode="ab")
        if args["outfile"] is not None
        else nullcontext(sys.stdout.buffer)
    )

    async with asyncio.TaskGroup() as tg:
        with ThreadPoolExecutor(1) as pool, outfile_contextmanager as f:
            loop = asyncio.get_running_loop()
            chunks: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)
            receive_task = tg.create_task(  # pyright: ignore[reportUnusedVariable]  # noqa: F841
                receive_forever(chunks, host, port),
            )
            while True:
                chunk = await chunks.get()
                _ = await loop.run_in_executor(pool, f.write, chunk)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()

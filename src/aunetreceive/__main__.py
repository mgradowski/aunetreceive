import asyncio
import ipaddress
import logging
import sys
from argparse import ArgumentParser
from typing import TypedDict, cast

from zeroconf.asyncio import AsyncZeroconf

import alsaaudio

from ._play import play_forever
from ._receive import (
    DEFAULT_AUNETSEND_PORT,
    receive_forever,
)

BONJOUR_SERVICE_TYPE: str = "_apple-ausend._tcp.local."
DEFAULT_AUNETSEND_BONJOUR_NAME: str = "AUNetSend"


_log = logging.getLogger(__name__)


class _Args(TypedDict):
    host: str | None
    port: int | None
    bonjour_name: str | None
    period_size: int
    n_periods: int
    device: str


async def _amain() -> None:
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
        help="AUNetSend Bonjour name for automatic host and port resolution.",
    )
    _ = argparser.add_argument(
        "-s",
        "--period-size",
        type=int,
        default=64,
        help="Number of frames in an ALSA period.",
    )
    _ = argparser.add_argument(
        "-n",
        "--n-periods",
        type=int,
        default=64,
        help="Number of periods in the ALSA buffer.",
    )
    _ = argparser.add_argument(
        "-D",
        "--device",
        type=str,
        default="default",
        help="Name of ALSA device to use for playback, e.g. 'plughw:2,0'.",
    )
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

    device = alsaaudio.PCM(
        device=args["device"],
        channels=2,
        rate=48000,
        format=alsaaudio.PCM_FORMAT_S16_LE,
        periodsize=args["period_size"],
        periods=args["n_periods"],
    )

    async with asyncio.TaskGroup() as tg:
        chunks: asyncio.Queue[bytes] = asyncio.Queue()
        receive = tg.create_task(  # pyright: ignore[reportUnusedVariable]  # noqa: F841
            receive_forever(chunks, host, port),
        )
        play = tg.create_task(play_forever(chunks, device))  # pyright: ignore[reportUnusedVariable]  # noqa: F841


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()

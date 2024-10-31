#!/usr/bin/env python3

import time
from typing import Annotated, Optional
#from asyncio import run as aiorun

import stream
from pynng import Pull0, Timeout # type: ignore[import-untyped]
import typer

from .nng import puller
from .stream_utils import clock

def psana_pull(
        listen: Annotated[
            Optional[str],
            typer.Option("--listen", "-l", help="Address to listen at (URL format)."),
        ] = None,
        dial: Annotated[
            Optional[str],
            typer.Option("--dial", "-d", help="Address to dial (URL format)."),
        ] = None,
    ):

    assert (dial is not None) or (listen is not None), "Need an address."
    ndial = 0
    if listen is None:
        ndial = 1 # need to dial
        addr = dial
    else:
        addr = listen

    # TODO: send to file_writer or something instead of len
    stats = puller(addr, ndial) >> stream.map(len) >> clock()
    # TODO: update tqdm progress meter
    for items in stats >> stream.item[1::10]:
        #print(items)
        print(f"At {items['count']}, {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.")
    try:
        final = stats >> stream.last(-1)
    except IndexError:
        final = items
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    print(f"Received {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.")

def run():
    typer.run(psana_pull)

if __name__ == "__main__":
    run()

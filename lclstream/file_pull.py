#!/usr/bin/env python3

import time
from typing import Annotated, Iterable, Optional
from collections.abc import Iterable, Iterator
from pathlib import Path
#from asyncio import run as aiorun

import stream
from pynng import Pull0, Timeout # type: ignore[import-untyped]
import typer

from .nng import puller
from .stream_utils import (
    decode_offset,
    file_writer,
    chunk_progress,
    clock
)

def create_file(fname, sz):
    # Create a file with a given size.
    with open(fname, mode="wb") as f:
        f.truncate(sz)

@stream.stream
def mkfile(chunks: Iterator[bytes], fname) -> Iterator[int]:
    first = next(chunks)
    sz, rem = decode_offset(first)
    assert len(rem) == 0, "Invalid first message (size required)."
    print(sz)
    create_file(fname, sz)
    yield from (chunks
                >> chunk_progress(sz, desc="Downloading")
                >> stream.map(decode_offset)
                >> file_writer(fname, size=sz))

def file_pull(
        addr: Annotated[str,
            typer.Argument(help="Address to dial/listen at (URL format)."),
        ],
        out: Annotated[Path,
            typer.Argument(help="Output file."),
        ],
        listen: Annotated[
            bool,
            typer.Option("--listen", "-l", help="Listen instead of dial?"),
        ] = False,
    ):

    ndial = 1
    if listen:
        ndial = 0

    stats = puller(addr, ndial) \
            >> mkfile(out) \
            >> clock()
    #for items in stats >> stream.item[1::10]:
    #    #print(items)
    #    print(f"At {items['count']}, {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.")
    #try:
    #    final = stats >> stream.last(-1)
    #except IndexError:
    #    final = items
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}

    final = stats >> stream.last()
    print(f"Received {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.")

def run():
    typer.run(file_pull)

if __name__ == "__main__":
    run()

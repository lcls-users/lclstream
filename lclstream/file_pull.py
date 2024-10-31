#!/usr/bin/env python3

import time
from typing import Annotated, Iterable, Optional
#from asyncio import run as aiorun

import stream
from pynng import Pull0, Timeout # type: ignore[import-untyped]
import typer

from .nng import (
    puller,
    decode_offset, file_writer,
    chunk_progress,
    rate_clock, clock0
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
    create_file(fname, sz)
    yield from (chunks
                >> chunk_progress(sz)
                >> stream.map(decode_offset)
                >> file_writer(fname, size=sz))

def file_pull(
        out: Annotated[Path,
            typer.Argument(help="Output file."),
        ],
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

    clock = stream.fold(rate_clock, clock0())
    stats = puller(addr, ndial) \
            >> mkfile(out) \
            >> clock
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

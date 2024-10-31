#!/usr/bin/env python3

from typing import Annotated, List
from collections.abc import Iterable, Iterator
from pathlib import Path
import hashlib

import stream

import numpy as np
import typer

from .nng import pusher
from .stream_utils import (
    clock,
    file_chunks,
    prepend_offset,
    chunk_progress,
    encode_offset
)

"""
try:
    from mpi4py import MPI
    rank  = MPI.COMM_WORLD.Get_rank()
    procs = MPI.COMM_WORLD.Get_size()
except:
    rank = 0
    procs = 1
"""

@stream.stream
def append_hash(it, alg='sha256'):
    yield next(it)
    h = hashlib.new(alg)
    for x in it:
        h.update(x[4:])
        yield x
    yield encode_offset(-1) + h.hexdigest().encode('ascii')

def file_push(
        name: Annotated[
            Path,
            typer.Argument(help="File path."),
        ],
        addr: Annotated[str,
            typer.Argument(help="Address to dial/listen at (URL format)."),
        ],
        dial: Annotated[
            bool,
            typer.Option("--dial", "-d", help="Dial-out to address?"),
        ] = False,
    ):

    ndial = 0
    if dial:
        ndial = 1

    sz = name.stat().st_size
    sends = stream.Source([ (sz, b'') ])
    sends << file_chunks(name)
    
    messages = sends \
               >> stream.apply(prepend_offset) \
               >> chunk_progress(sz, desc="Uploading") \
               >> append_hash() \
               >> pusher(addr, ndial) \
               >> clock()
    #for items in stats >> stream.item[1::32]:
    #    print(f"At {items['count']} messages in {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.")
    #try:
    #    final = stats >> stream.last(-1)
    #except IndexError:
    #    final = items

    # run the stream
    final = messages >> stream.last()
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    print(f"Sent {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.")

    return 0

def run():
    typer.run(file_push)

if __name__ == "__main__":
    run()

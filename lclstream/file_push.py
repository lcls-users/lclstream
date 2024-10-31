#!/usr/bin/env python3

from typing import Annotated, List
from collections.abc import Iterable, Iterator
from pathlib import Path

import stream

import numpy as np
import typer

from .nng import (
    pusher, rate_clock, clock0,
    file_chunks, prepend_offset, chunk_progress
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

def file_push(
        name: Annotated[
            Path,
            typer.Argumen(help="File path."),
        ],
    ):
    sz = name.stat().st_size
    sends = stream.Stream([sz, b''])
    sends << file_chunks(name)
    
    messages = sends >> stream.apply(prepend_offset) \
               >> chunk_progress(sz) \
               >> pusher(addr, 1) \
               >> stream.fold(rate_clock, clock0())
    #for items in stats >> stream.item[1::32]:
    #    print(f"At {items['count']} messages in {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.")
    #try:
    #    final = stats >> stream.last(-1)
    #except IndexError:
    #    final = items

    # run the stream
    final = stats >> stream.last()
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    print(f"Sent {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.")

    return 0

def run():
    typer.run(file_push)

if __name__ == "__main__":
    run()

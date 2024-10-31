#!/usr/bin/env python3

from io import BytesIO
from typing import Annotated, List
from collections.abc import Iterable, Iterator

import stream

import numpy as np
import h5py # type: ignore[import-untyped]
import hdf5plugin # type: ignore[import-untyped]
import typer

from .models import ImageRetrievalMode, AccessMode
from .psana_img_src import PsanaImgSrc
from .nng import pusher
from .stream_utils import clock

"""
try:
    from mpi4py import MPI
    rank  = MPI.COMM_WORLD.Get_rank()
    procs = MPI.COMM_WORLD.Get_size()
except:
    rank = 0
    procs = 1
"""

Tensor = np.ndarray # type alias

def Hdf5FileWriter(ilist: List[Tensor]) -> bytes:
    """ This creates an in-memory hdf5-format file.

    Returns a serialized hdf5 (bytes)  containing several images.

    Params:
        ilist: list of image arrays, all the same shape
    """
    if len(ilist) == 0:
        return b'' # return h5py.File() with no dataset?
    with BytesIO() as f:
        with h5py.File(f, 'w') as fh:
            dataset = fh.create_dataset(
                'data',
                shape = (len(ilist),) + ilist[0].shape,
                **hdf5plugin.Zfp()
            )
            for idx, img in enumerate(ilist):
                dataset[idx] = img

        return f.read()

def psana_push(
        experiment: Annotated[
            str,
            typer.Option("--experiment", "-e", help="Experiment identifier"),
        ],
        run: Annotated[
            int,
            typer.Option("--run", "-r", help="Run number"),
        ],
        detector: Annotated[
            str,
            typer.Option("--detector", "-d", help="Detector name"),
        ],
        mode: Annotated[
            ImageRetrievalMode,
            typer.Option("--mode", "-m", help="Image retrieval mode"),
        ],    
        addr: Annotated[
            str,
            typer.Option("--addr", "-a", help="Destination address (URL format)."),
        ],
        access_mode: Annotated[
            AccessMode,
            typer.Option("--access_mode", "-c", help="Data access mode"),
        ],
        img_per_file: Annotated[
            int,
            typer.Option("--img_per_file", "-n", help="Number of images per file"),
        ] = 20,
    ):
    ps = PsanaImgSrc(experiment, run, access_mode, detector)

    messages = ps(mode) >> stream.chop(img_per_file) >> stream.map(Hdf5FileWriter) # iterator over hdf5 bytes

    stats = messages >> pusher(addr, 1) >> clock()
    # TODO: update tqdm progress meter
    for items in stats >> stream.item[1::32]:
        print(f"At {items['count']} messages in {items['wait']} seconds: {items['size']/items['wait']/1024**2} MB/sec.")
    try:
        final = stats >> stream.last(-1)
    except IndexError:
        final = items
    # {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
    print(f"Sent {final['count']} messages in {final['wait']} seconds: {final['size']/final['wait']/1024**2} MB/sec.")

    return 0

def run():
    typer.run(psana_push)

if __name__ == "__main__":
    run()

#!/usr/bin/env python3

from typing import Annotated

from pynng import Push0 # type: ignore[import-untyped]
import typer
import zfpy # type: ignore[import-untyped]

from lclstream.psana_img_src import PsanaImgSrc
from lclstream.models import ImageRetrievalMode, AccessMode

def serialize(data) -> bytes:
    return zfpy.compress_numpy(data, write_header=True)

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

):
    ps = PsanaImgSrc(experiment, run, access_mode, detector)

    send_opts = {
        "send_buffer_size": 32 # send blocks if 32 messages queue up
    }
    with Push0(dial=addr, **send_opts) as push:
        print(f"Connected to {addr} - starting stream.")
        for img in ps(mode):
            buf = serialize(img)
            push.send(buf)

def run():
    typer.run(psana_push)

if __name__ == "__main__":
    run()

from typing import Tuple
from collections.abc import Iterator
import time
import logging
_logger = logging.getLogger(__name__)

import stream
from pynng import Push0, Pull0, Timeout, ConnectionRefused # type: ignore[import-untyped]

from tqdm import tqdm
@stream.stream
def chunk_progress(it, sz):
    with tqdm(desc="Downloading", total=sz) as pbar:
        for x in it:
            yield x
            pbar.update(len(x)-4)

clock0 = lambda: {'count': 0, 'size': 0, 'wait': 0, 'time': time.time()}
def rate_clock(state, sz):
    t = time.time()
    return {
        'count': state['count'] + 1,
        'size': state['size'] + sz,
        'wait': state['wait'] + t - state['time'],
        'time': t
    }

send_opts : dict[str,int] = {
     #"send_buffer_size": 32 # send blocks if 32 messages queue up
}
recv_options = {"recv_timeout": 5000}

@stream.stream
def pusher(gen : Iterator[bytes], addr : str, ndial : int
          ) -> Iterator[int]:
    # transform messages sent into sizes sent
    assert ndial >= 0
    options = dict(send_opts)
    if ndial == 0:
        options["listen"] = addr
    try:
        with Push0(**options) as push:
            for dial in range(ndial):
                push.dial(addr, block=True)
            if ndial > 0:
                _logger.info("Connected to %s x %d - starting stream.",
                             addr, ndial)
            else:
                _logger.info("Listening on %s.", addr)

            for msg in gen:
                push.send(msg)
                yield len(msg)
    except ConnectionRefused as e:
        _logger.error("Unable to connect to %s - %s", addr, e)

@stream.source
def puller(addr : str, ndial : int) -> Iterator[bytes]:
    assert ndial >= 0

    done = 0
    started = 0
    def show_open(pipe):
        nonlocal started
        _logger.info("Pull: pipe opened")
        started += 1
    def show_close(pipe):
        nonlocal done
        _logger.info("Pull: pipe closed")
        done += 1

    options = dict(recv_options)
    if ndial == 0:
        options["listen"] = addr
    try:
        with Pull0(**options) as pull:
            pull.add_post_pipe_connect_cb(show_open)
            pull.add_post_pipe_remove_cb(show_close)
            for dial in range(ndial):
                pull.dial(addr, block=True)
            if ndial == 0:
                _logger.info("Pull: waiting for connection")
            else:
                _logger.info("Connected to %s x %d - starting recv.",
                              addr, ndial)

            while started == 0 or (done != started):
                try:
                    msg = pull.recv()
                    yield msg
                except Timeout:
                    if started:
                        _logger.debug("Pull: slow input")

    except ConnectionRefused as e:
        _logger.error("Unable to connect to %s - %s", addr, e)

def encode_offset(offset: int) -> bytes:
    return struct.pack('!i', offset)
def decode_offset(ochunk: bytes) -> Tuple[int, bytes]:
    assert len(ochunk) >= 4, "Unable to decode offset."
    return struct.unpack('!i', ochunk[:4]), ochunk[4:]
def prepend_offset(off: int, chunk: bytes) -> bytes:
    return encode_offset(off)+chunk
#glob_offset = stream.apply(prepend_offset)
#get_offset = stream.map(decode_offset)

@stream.source
def file_chunks(fname, chunksz=1024*1024
               ) -> Iterator[Tuple[int,bytes]]:
    with open(fname, 'rb') as f:
        offset = 0
        while True:
            data = f.read(chunksz)
            if len(data) == 0:
                break
            yield offset, data
            offset += len(data)

@stream.stream
def file_writer(gen: Iterator[Tuple[int,bytes]],
                fname: str,
                size: int = 0,
                append: bool = False,
               ) -> Iterator[int]:
    if append:
        mode = 'ab'
    else:
        mode = 'wb'
    with open(fname, mode) as f:
        for off, data in gen:
            if not append:
                if off < 0:
                    raise ValueError("Invalid (negative) offset")
                if size > 0 and off+len(data) > sz:
                    raise ValueError("Refusing to write beyond end of file.")
                f.seek(off)
            yield f.write(data)

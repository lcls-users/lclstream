from typing import Tuple, Union
from collections.abc import Iterator
import hashlib
import os
from pathlib import Path
import time
import struct
import logging
_logger = logging.getLogger(__name__)

import stream
from tqdm import tqdm

@stream.stream
def chunk_progress(it, sz, desc=None):
    """ Create a progress-bar to count bytes sent.
    """
    with tqdm(desc=desc, total=sz) as pbar:
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

def clock():
    """ Return a rate-clock counting from now.
    This transforms a stream of counts into a stream
    of dicts (describing the count rate).
    """
    return stream.fold(rate_clock, clock0())

@stream.stream
def hasher(it : Iterator[bytes], alg='sha256'
          ) -> Iterator[Union[bytes,str]]:
    """ Accumulate a hash of all the (bytes) moving through
    this stream.  The last message from the stream
    will be a string, containing the stream's hash digest.
    """
    h = hashlib.new(alg)
    for x in it:
        h.update(x)
        yield x
    yield h.hexdigest()

def hash_file(fname: Union[str,os.PathLike], alg='sha256') -> str:
    """ Read a file and compute its hash.
    """
    return file_chunks(fname) >> stream.cut[1] \
            >> hasher(alg) >> stream.last()

def encode_offset(offset: int) -> bytes:
    return struct.pack('!i', offset)
def decode_offset(ochunk: bytes) -> Tuple[int, bytes]:
    assert len(ochunk) >= 4, "Unable to decode offset."
    return struct.unpack('!i', ochunk[:4])[0], ochunk[4:]
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
    pos = 0
    off = 0
    with open(fname, mode) as f:
        for off, data in gen:
            if not append:
                if off == -1: # a final hash!
                    break
                if off < 0:
                    raise ValueError("Invalid (negative) offset")
                if size > 0 and off+len(data) > size:
                    raise ValueError("Refusing to write beyond end of file.")
                if off != pos:
                    f.seek(off)
                    pos = off
            yield f.write(data)
            pos += len(data)

    if off == -1:
        H = hash_file(fname)
        assert H == data.decode('ascii')
        _logger.warning("File checksum matches!")
    else:
        _logger.error("Error: no file checksum received.")

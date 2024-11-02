from typing import TypeVar, Optional
from collections.abc import Iterator, Callable
import io
import time
import logging
_logger = logging.getLogger(__name__)

import stream
import h5py
from pynng import Push0, Pull0, Timeout, ConnectionRefused # type: ignore[import-untyped]

send_opts : dict[str,int] = {
     #"send_buffer_size": 32 # send blocks if 32 messages queue up
}
recv_options = {"recv_timeout": 5000}

T = TypeVar('T')
def load_h5(buf: bytes, reader: Callable[[h5py.File],T]) -> Optional[T]:
    """ Simple function to read an hdf5 file from
    its serialized bytes representation.

    Returns the result of calling `reader(h5file)`
    or None on error.
    """
    try:
        with io.BytesIO(buf) as f:
            with h5py.File(f, 'r') as h:
                return reader(h)
    except (IOError, OSError):
        pass
    return None

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

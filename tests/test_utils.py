import random

import stream

from lclstream.stream_utils import (
    hash_file,
    file_writer
)

def test_hash(tmpdir):
    random.seed(12)
    fname = tmpdir/"12.bin"

    def accum_sz(it):
        sz = 0
        for x in it:
            yield sz, x
            sz += len(x)
    content = stream.repeatcall(random.randbytes, 1024) \
                >> stream.Stream(accum_sz) \
                >> file_writer(fname)
    out = content >> stream.take(32) >> stream.last()

    H = hash_file(fname)
    assert H == 'f8d82ce7a4af1c298b611c4cfa2552cae21e0b565695ab1c40979593f2af0bc5'



from typing import Any
from collections.abc import Iterator
import tarfile
import io

import stream

@stream.sink
def write_tar(inp: Iterator[bytes], file: Any, names: str
             ) -> None:
    with tarfile.open(fileobj=file.buffer, mode="w") as tar:
        for i, data in enumerate(inp):
            # Create a TarInfo object for the file
            info = tarfile.TarInfo(names % i)
            info.size = len(data)

            # Create a BytesIO object to hold the file content
            file_obj = io.BytesIO(data)

            # Add the file to the tar archive
            tar.addfile(info, fileobj=file_obj)

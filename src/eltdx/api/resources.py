"""TDX server file API."""

from __future__ import annotations

from eltdx.protocol.constants import DEFAULT_FILE_CHUNK_SIZE

from .base import ApiBase


class ResourceApi(ApiBase):
    def read(self, path: str, *, offset: int = 0, size: int = DEFAULT_FILE_CHUNK_SIZE):
        """Read one byte range from a file exposed by the TDX server."""

        return self._execute("file_content", path=path, offset=offset, size=size)

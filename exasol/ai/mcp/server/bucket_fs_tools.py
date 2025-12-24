from typing import Any
import tempfile
import requests  # type: ignore
import exasol.bucketfs as bfs  # type: ignore

from exasol.ai.mcp.server.keyword_search import keyword_filter
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)

PATH_FIELD = "FULL_PATH"
NAME_FIELD = "NAME"
IS_DIR_FIELD = "IS_DIR"
IS_FILE_FIELD = "IS_FILE"


class BucketFsTools:
    def __init__(self, bfs_location: bfs.path.PathLike, config: McpServerSettings):
        self.bfs_location = bfs_location
        self.config = config

    def list_items(self, rel_dir: str = '') -> ExaDbResult:
        """
        Lists files and subdirectories at the given directory. The directory path
        is relative to the root location.
        """
        abs_dir = self.bfs_location.joinpath(rel_dir)
        content = [
            {
                PATH_FIELD: str(pth),
                NAME_FIELD: pth.name,
                IS_DIR_FIELD: pth.is_dir(),
                IS_FILE_FIELD: pth.is_file(),
            }
            for pth in abs_dir.iterdir()
        ]
        return ExaDbResult(content)

    def find_items(self, keywords: list[str], rel_path: str = '') -> ExaDbResult:
        """
        Performs a keyword search of files and directories, starting from a given path.
        The path is relative to the root location.
        """
        abs_dir = self.bfs_location.joinpath(rel_path)
        content: list[dict[str, Any]] = []
        for dir_path, dir_names, file_names in abs_dir.walk():
            for dir_name in dir_names:
                content.append({
                    PATH_FIELD: str(dir_path.joinpath(dir_name)),
                    NAME_FIELD: dir_name,
                    IS_DIR_FIELD: True,
                    IS_FILE_FIELD: False,
                })
            for file_name in file_names:
                content.append({
                    PATH_FIELD: str(dir_path.joinpath(file_name)),
                    NAME_FIELD: file_name,
                    IS_DIR_FIELD: False,
                    IS_FILE_FIELD: True,
                })
        return ExaDbResult(
            keyword_filter(content, keywords, language=self.config.language)
        )

    def read_file(self, rel_path: str) -> str:
        """
        Reads the content of a text file at the provided path in bucket-fs and returns
        it as a string. The path is relative to the root location.
        """
        abs_path = self.bfs_location.joinpath(rel_path)
        if not abs_path.is_file():
            raise FileNotFoundError(abs_path)
        byte_content = b"".join(abs_path.read())
        return str(byte_content, encoding="utf-8")

    def write_file(self, rel_path: str, content: str) -> None:
        """
        Writes a piece of text to a file at the provided path in bucket-fs.
        The path is relative to the root location. The file overrides an existing file.
        """
        abs_path = self.bfs_location.joinpath(rel_path)
        byte_content = content.encode(encoding="utf-8")
        abs_path.write(byte_content)

    def download_file(self, rel_path: str, url: str) -> None:
        """
        Downloads a file from a given url and writes to a file at the provided path in
        bucket-fs. The path is relative to the root location. The file overrides an
        existing file.
        """
        abs_path = self.bfs_location.joinpath(rel_path)
        with tempfile.NamedTemporaryFile() as tmp_file:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            tmp_file.write(response.content)
            tmp_file.flush()
            # tmp_file.seek(0)
            # abs_path.write(tmp_file)
            # Have to open another file handler due to a bug in bucketfs-python
            # https://github.com/exasol/bucketfs-python/issues/262
            with open(tmp_file.name) as f:
                abs_path.write(f)

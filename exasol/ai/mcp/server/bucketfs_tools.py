from collections.abc import Callable

import exasol.bucketfs as bfs

from exasol.ai.mcp.server.keyword_search import keyword_filter
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)

PATH_FIELD = "FULL_PATH"


class BucketFsTools:
    def __init__(self, bfs_location: bfs.path.PathLike, config: McpServerSettings):
        self.bfs_location = bfs_location
        self.config = config

    def _list_items(
        self, rel_dir: str, item_filter: Callable[[bfs.path.PathLike], bool]
    ) -> ExaDbResult:
        abs_dir = self.bfs_location.joinpath(rel_dir)
        content = [
            {PATH_FIELD: str(pth)}
            for pth in abs_dir.iterdir()
            if item_filter(pth)
        ]
        return ExaDbResult(content)

    def list_directories(self, directory: str = "") -> ExaDbResult:
        """
        Lists subdirectories at the given directory. The directory path is relative
        to the root location.
        """
        return self._list_items(directory, lambda pth: pth.is_dir())

    def list_files(self, directory: str = "") -> ExaDbResult:
        """
        Lists files at the given directory. The directory path is relative to the
        root location.
        """
        return self._list_items(directory, lambda pth: pth.is_file())

    def find_files(self, keywords: list[str], directory: str = "") -> ExaDbResult:
        """
        Performs a keyword search of files at the given directory and all its descendant
        subdirectories. The path is relative to the root location.
        """
        abs_dir = self.bfs_location.joinpath(directory)
        content = [
            {PATH_FIELD: str(dir_path.joinpath(file_name))}
            for dir_path, dir_names, file_names in abs_dir.walk()
            for file_name in file_names
        ]
        return ExaDbResult(
            keyword_filter(content, keywords, language=self.config.language)
        )

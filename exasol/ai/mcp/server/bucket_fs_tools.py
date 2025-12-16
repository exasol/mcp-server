from typing import Any
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

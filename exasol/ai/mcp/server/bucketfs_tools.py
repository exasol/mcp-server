from typing import Annotated
from collections.abc import Callable

from pydantic import (
    BaseModel,
    Field,
)
import tempfile
import requests
import exasol.bucketfs as bfs
from fastmcp import Context

from exasol.ai.mcp.server.keyword_search import keyword_filter
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)

PATH_FIELD = "FULL_PATH"
NAME_FIELD = "NAME"


class BucketFsTools:
    def __init__(self, bfs_location: bfs.path.PathLike, config: McpServerSettings):
        self.bfs_location = bfs_location
        self.config = config

    def _list_items(
        self, rel_dir: str, item_filter: Callable[[bfs.path.PathLike], bool]
    ) -> ExaDbResult:
        abs_dir = self.bfs_location.joinpath(rel_dir)
        content = [
            {
                PATH_FIELD: str(pth),
                NAME_FIELD: pth.name,
            }
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
            {
                PATH_FIELD: str(dir_path.joinpath(file_name)),
                NAME_FIELD: file_name,
            }
            for dir_path, dir_names, file_names in abs_dir.walk()
            for file_name in file_names
        ]
        return ExaDbResult(
            keyword_filter(content, keywords, language=self.config.language)
        )

    def read_file(self, path: str) -> str:
        """
        Reads the content of a text file at the provided path in bucket-fs and returns
        it as a string. The path is relative to the root location.
        """
        abs_path = self.bfs_location.joinpath(path)
        if not abs_path.is_file():
            raise FileNotFoundError(abs_path)
        byte_content = b"".join(abs_path.read())
        return str(byte_content, encoding="utf-8")

    async def write_file(
        self,
        path: Annotated[str, Field(description="Path to save the file")],
        content: Annotated[str, Field(description="File content")],
        ctx: Context
    ) -> None:
        """
        Writes a piece of text to a file at the provided path in bucket-fs.
        The path is relative to the root location. An existing file will be overwritten.
        Elicitation is required. If the path is modified in elicitation and there is an
        existing file at the modified path, the elicitation is repeated, to get an
        explicit confirmation that the existing file can be deleted.
        """
        abs_path = self.bfs_location.joinpath(path)
        file_exists = abs_path.exists()
        while True:
            class FileElicitation(BaseModel):
                file_path: str = Field(default=path)
                file_content: str = Field(default=content)

            message = (
                "The following text will be saved in a BucketFS file at the give path. "
                "Please review the text and the path. Make changes if need. Finally, "
                "accept or decline the operation."
            )
            if file_exists:
                message += (
                    " Please note that there is an existing file at the chosen path. "
                    "If the operation is accepted the existing file will be overwritten."
                )
            confirmation = await ctx.elicit(
                message=message,
                response_type=FileElicitation,
            )
            if confirmation.action == "accept":
                accepted_path = confirmation.data.file_path
                content = confirmation.data.file_content
                abs_path = self.bfs_location.joinpath(accepted_path)
                if accepted_path != path:
                    file_exists = abs_path.exists()
                    if file_exists:
                        path = accepted_path
                        continue
                byte_content = content.encode(encoding="utf-8")
                abs_path.write(byte_content)
                break
            elif confirmation.action == "reject":
                raise InterruptedError("The query execution is declined by the user.")
            else:  # cancel
                raise InterruptedError("The query execution is cancelled by the user.")

    def download_file(self, file_path: str, url: str) -> None:
        """
        Downloads a file from a given url and writes to a file at the provided path in
        bucket-fs. The path is relative to the root location. The file overwrites an
        existing file.
        """
        abs_path = self.bfs_location.joinpath(file_path)
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

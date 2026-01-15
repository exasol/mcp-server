import asyncio
import tempfile
from collections.abc import Callable
from contextlib import AsyncExitStack
from enum import (
    Enum,
    auto,
)
from typing import Annotated

import exasol.bucketfs as bfs
import httpx
from aiofile import async_open
from fastmcp import Context
from pathvalidate import (
    ValidationError,
    validate_filepath,
)
from pydantic import (
    BaseModel,
    Field,
)

from exasol.ai.mcp.server.keyword_search import keyword_filter
from exasol.ai.mcp.server.server_settings import (
    ExaDbResult,
    McpServerSettings,
)


class PathStatus(Enum):
    OK = auto()
    Invalid = auto()
    FileExists = auto()
    DirExists = ()


PATH_FIELD = "FULL_PATH"

PATH_WARNINGS = {
    PathStatus.FileExists: (
        "Please note that there is an existing file at the chosen path. If the "
        "operation is accepted the existing file will be overwritten."
    ),
    PathStatus.DirExists: (
        "There is an existing directory at the chosen path. The operation cannot "
        "proceed because it is not possible to delete a directory. Please choose "
        "another path."
    ),
    PathStatus.Invalid: (
        "Please note that the chosen path has some invalid characters and must be "
        "modified."
    ),
}


class BucketFsTools:
    def __init__(self, bfs_location: bfs.path.PathLike, config: McpServerSettings):
        self.bfs_location = bfs_location
        self.config = config

    def _list_items(
        self, directory: str, item_filter: Callable[[bfs.path.PathLike], bool]
    ) -> ExaDbResult:
        abs_dir = self.bfs_location.joinpath(directory)
        content = [
            {PATH_FIELD: str(pth)} for pth in abs_dir.iterdir() if item_filter(pth)
        ]
        return ExaDbResult(content)

    def _get_path_status(self, path: str) -> PathStatus:
        # First check if the path has any of the BucketFS own disallowed characters.
        if any(c in path for c in ": "):
            return PathStatus.Invalid
        # Then check the normal Linux rules.
        try:
            validate_filepath(path, platform="Linux")
        except ValidationError:
            return PathStatus.Invalid
        bfs_path = self.bfs_location.joinpath(path)
        # If the path is OK, check if it points to an existing file or directory.
        if bfs_path.is_file():
            return PathStatus.FileExists
        elif bfs_path.is_dir():
            return PathStatus.DirExists
        return PathStatus.OK

    async def _elicitate(self, message: str, ctx: Context, response_type_factory):

        path, response_type = response_type_factory()
        path_status = self._get_path_status(path)
        while True:
            if path_status == PathStatus.OK:
                full_message = message
            else:
                full_message = f"{message} {PATH_WARNINGS[path_status]}"
            confirmation = await ctx.elicit(
                message=full_message,
                response_type=response_type,
            )
            if confirmation.action == "accept":
                accepted_path, response_type = response_type_factory(confirmation.data)
                path_status = self._get_path_status(accepted_path)
                if path_status == PathStatus.DirExists or (
                    (path_status == PathStatus.FileExists) and (accepted_path != path)
                ):
                    # The chosen path points to an existing directory (we cannot
                    # proceed with that), or an existing file (OK, but we need an
                    # explicit confirmation in order to proceed). Either way, go
                    # for another elicitation.
                    path = accepted_path
                    continue
                return confirmation.data
            elif confirmation.action == "reject":
                raise InterruptedError("The file operation is declined by the user.")
            else:  # cancel
                raise InterruptedError("The file operation is cancelled by the user.")

    def list_directories(
        self,
        directory: Annotated[
            str, Field(description="Directory, defaults to bucket root", default="")
        ],
    ) -> ExaDbResult:
        """
        Lists subdirectories at the given directory. The directory path is relative
        to the root location.
        """
        return self._list_items(directory, lambda pth: pth.is_dir())

    def list_files(
        self,
        directory: Annotated[
            str, Field(description="Directory, defaults to bucket root", default="")
        ],
    ) -> ExaDbResult:
        """
        Lists files at the given directory. The directory path is relative to the
        root location.
        """
        return self._list_items(directory, lambda pth: pth.is_file())

    def find_files(
        self,
        keywords: Annotated[
            list[str],
            Field(description="List of keywords to look for in the file path"),
        ],
        directory: Annotated[
            str, Field(description="Directory, defaults to bucket root", default="")
        ],
    ) -> ExaDbResult:
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

    def read_file(
        self, path: Annotated[str, Field(description="Full path of the file")]
    ) -> str:
        """
        Reads the content of a text file at the provided path in BucketFS and returns
        it as a string. The path is relative to the root location.
        """
        abs_path = self.bfs_location.joinpath(path)
        if not abs_path.is_file():
            raise FileNotFoundError(abs_path)
        byte_content = b"".join(abs_path.read())
        return str(byte_content, encoding="utf-8")

    async def write_text_to_file(
        self,
        path: Annotated[
            str,
            Field(
                description=(
                    "BucketFS file path where the file should be saved. "
                    "Spaces and colons are not allowed in the path."
                )
            ),
        ],
        content: Annotated[str, Field(description="File textual content")],
        ctx: Context,
    ) -> None:
        """
        Writes a piece of text to a file at the provided path in BucketFS.
        The path is relative to the root location. An existing file will be overwritten.
        Elicitation is required. If the path is modified in elicitation and there is an
        existing file at the modified path, the elicitation is repeated, to get an
        explicit confirmation that the existing file can be deleted.
        """

        def response_type_factory(data=None):
            nonlocal path, content
            if data is not None:
                path = data.file_path
                content = data.file_content

            class FileElicitation(BaseModel):
                file_path: str = Field(default=path)
                file_content: str = Field(default=content)

            return path, FileElicitation

        message = (
            "The following text will be saved in a BucketFS file at the give path. "
            "Please review the text and the path. Make changes if needed. Finally, "
            "accept or decline the operation."
        )

        answer = await self._elicitate(message, ctx, response_type_factory)
        abs_path = self.bfs_location.joinpath(answer.file_path)
        byte_content = answer.file_content.encode(encoding="utf-8")
        abs_path.write(byte_content)

    async def download_file(
        self,
        path: Annotated[
            str,
            Field(
                description=(
                    "BucketFS file path where the file should be saved. "
                    "Spaces and colons are not allowed in the path."
                )
            ),
        ],
        url: Annotated[
            str, Field(description="URL where the file should be downloaded from")
        ],
        ctx: Context,
    ) -> None:
        """
        Downloads a file from a given url and writes to a file at the provided path in
        BucketFS. The path is relative to the root location. The file overwrites an
        existing file.
        """

        def response_type_factory(data=None):
            nonlocal path
            if data is not None:
                path = data.file_path

            class FileElicitation(BaseModel):
                file_path: str = Field(default=path)

            return path, FileElicitation

        message = (
            f"The file at {url} will be downloaded and saved in a BucketFS file "
            "at the give path. The path can be changed if need. Please accept or "
            "decline the operation."
        )

        answer = await self._elicitate(message, ctx, response_type_factory)
        abs_path = self.bfs_location.joinpath(answer.file_path)

        with tempfile.NamedTemporaryFile() as tmp_file:
            async with AsyncExitStack() as stack:
                client = await stack.enter_async_context(httpx.AsyncClient(timeout=300))
                response = await stack.enter_async_context(client.stream("GET", url))
                response.raise_for_status()

                # Download in chunks
                afp = await stack.enter_async_context(async_open(tmp_file.name, "wb"))
                async for chunk in response.aiter_bytes(262144):
                    await afp.write(chunk)

            # At the moment, BucketFS only supports synchronous I/O
            def upload_to_bucketfs():
                with open(tmp_file.name) as f:
                    abs_path.write(f)

            await asyncio.to_thread(upload_to_bucketfs)

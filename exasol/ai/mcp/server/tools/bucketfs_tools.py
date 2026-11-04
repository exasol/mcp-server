import asyncio
import os
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
from mcp.types import InputRequiredResult
from pathvalidate import (
    ValidationError,
    validate_filepath,
)
from pydantic import (
    BaseModel,
    Field,
)

from exasol.ai.mcp.server.setup.server_settings import (
    McpServerSettings,
)
from exasol.ai.mcp.server.tools.elicitation import (
    is_input_required_era,
    raise_if_not_accepted,
    read_input,
    read_request_state,
    request_input,
)
from exasol.ai.mcp.server.utils.keyword_search import keyword_filter


class PathStatus(Enum):
    Vacant = auto()
    Invalid = auto()
    FileExists = auto()
    DirExists = ()


PATH_FIELD = "FULL_PATH"

PATH_WARNINGS = {
    PathStatus.Vacant: "There is no file or directory at the chosen path.",
    PathStatus.FileExists: (
        "There is an existing file at the chosen path. If the operation is accepted "
        "the existing file will be overwritten."
    ),
    PathStatus.DirExists: (
        "There is an existing directory at the chosen path. The operation cannot "
        "proceed. Please choose another path."
    ),
    PathStatus.Invalid: (
        "Please note that the chosen path has some invalid characters and must be "
        "modified."
    ),
}

DirectoryArg = Annotated[str, Field(description="Full path of the BucketFS directory")]

OptionalDirectoryArg = Annotated[
    str,
    Field(
        description="Full path of the BucketFS directory. Defaults to bucket root.",
        default="",
    ),
]

FileArg = Annotated[str, Field(description="Full path of the BucketFS file")]

NewFileArg = Annotated[
    str,
    Field(
        description=(
            "Full path where the BucketFS file should be saved. "
            "Spaces and colons are not allowed in the path."
        )
    ),
]


def get_path_warning(
    path_status: PathStatus, expected_status: PathStatus | None
) -> str:
    """
    Returns a possible warning, depending on the path status and what status is
    expected. If the expected status is not specified then the warning is empty in case
    when neither file nor directory exists at the given path. Otherwise, when a certain
    status is expected, the warning is empty if the path status matches the expected.
    """
    if (
        (path_status == PathStatus.Vacant) and (expected_status is None)
    ) or path_status == expected_status:
        return ""
    return PATH_WARNINGS[path_status]


def compose_message(
    message: str, path_status: PathStatus, expected_status: PathStatus | None
) -> str:
    """
    Appends the elicitation message with a possible warning regarding the path.
    """
    warning = get_path_warning(path_status, expected_status)
    return f"{message} {warning}" if warning else message


def check_answer(
    accepted_path: str,
    prev_path: str,
    path_status: PathStatus,
    expected_status: PathStatus | None,
) -> bool:
    """
    Decides whether an accepted elicitation can be acted upon, or the elicitation
    needs to be repeated.

    Args:
        accepted_path:
            The path in the accepted elicitation data.
        prev_path:
            The path offered to the user in this elicitation.
        path_status:
            The status of the accepted path.
        expected_status:
            Optional status the path must have, as used by the deletion tools.
    """
    if expected_status is None:
        # A file (but not a directory) may exist at the chosen path,
        # in which case we need an explicit confirmation for this path.
        return (path_status == PathStatus.Vacant) or (
            (path_status == PathStatus.FileExists) and (accepted_path == prev_path)
        )
    # The chosen path must point to an existing file or directory,
    # as per the request.
    return path_status == expected_status


class BucketFsTools:
    def __init__(self, bfs_location: bfs.path.PathLike, config: McpServerSettings):
        self.bfs_location = bfs_location
        self.config = config

    def _list_items(
        self, directory: str, item_filter: Callable[[bfs.path.PathLike], bool]
    ) -> list[str]:
        abs_dir = self.bfs_location.joinpath(directory)
        return [str(pth) for pth in abs_dir.iterdir() if item_filter(pth)]

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
        return PathStatus.Vacant

    async def _elicitate(
        self,
        message: str,
        ctx: Context,
        response_type_factory,
        expected_status: PathStatus | None = None,
    ) -> BaseModel | InputRequiredResult:
        """
        Calls the MCP elicitation one or more times. The elicitation is required for
        data modifying tools, such as writing and deleting files. A mandatory field in
        the elicitation data is a BucketFS path of a file or directory targeted by the
        tool. The exact rules in regard to the path are different depending on the tool.
        Generally, if the user changed the path, and there is important information the
        user may need to know, for instance there is an existing file at this path, the
        elicitation may be repeated.

        Args:
            message:
                Generic message to be displayed in the elicitation. This message can be
                appended with a warning regarding the chosen path.
            ctx:
                MCP context.
            response_type_factory:
                Function creating a response_type to be passed to the elicitation. The
                function takes one optional parameter - the elicitation data. It should
                return a tuple - the path and the response type. The path comes either
                from the initial value or from a filed in the provided elicitation data.
            expected_status:
                Optional parameter used by the deletion tools. For these tools, the
                chosen path must point to an existing file or directory.

        Returns the accepted elicitation data. On the 2026-07-28 protocol, it can
        instead return an ``InputRequiredResult``, which the tool must return as is.
        """
        path, response_type = response_type_factory()
        if self.config.disable_elicitation:
            # If elicitation is disabled, then emulate the acceptance of the
            # default data.
            return response_type()
        if is_input_required_era(ctx):
            return self._elicitate_with_guard(
                message,
                ctx,
                response_type_factory,
                path,
                response_type,
                expected_status,
            )
        path_status = self._get_path_status(path)
        while True:
            confirmation = await ctx.elicit(
                message=compose_message(message, path_status, expected_status),
                response_type=response_type,
            )
            raise_if_not_accepted(confirmation.action, "The file operation")
            accepted_path, response_type = response_type_factory(confirmation.data)
            path_status = self._get_path_status(accepted_path)
            if check_answer(accepted_path, path, path_status, expected_status):
                return confirmation.data
            # At this point, we go for another elicitation.
            path = accepted_path

    def _elicitate_with_guard(
        self,
        message: str,
        ctx: Context,
        response_type_factory,
        path: str,
        response_type: type[BaseModel],
        expected_status: PathStatus | None,
    ) -> BaseModel | InputRequiredResult:
        """
        The guard pattern version of the elicitation loop in ``_elicitate``. Each
        round of the loop is a separate call of the tool, with the same arguments.
        If the elicitation needs to be repeated, the data offered to the user is saved
        in the request state, from where it is restored in the next round. The path
        status is checked again in every round.

        The path and response type are the initial ones, created by the
        ``response_type_factory`` from the tool arguments.
        """
        offered_data = read_request_state(ctx)
        if offered_data:
            path, response_type = response_type_factory(
                response_type.model_validate(offered_data)
            )
        user_input = read_input(ctx, response_type)
        if user_input is None:
            path_status = self._get_path_status(path)
            return request_input(
                compose_message(message, path_status, expected_status),
                response_type,
                offered_data,
            )
        raise_if_not_accepted(user_input.action, "The file operation")
        accepted_path, response_type = response_type_factory(user_input.data)
        path_status = self._get_path_status(accepted_path)
        if check_answer(accepted_path, path, path_status, expected_status):
            return user_input.data
        return request_input(
            compose_message(message, path_status, expected_status),
            response_type,
            user_input.data.model_dump(),
        )

    @staticmethod
    def _create_response_type_factory(path: str):
        """
        Creates a basic response type factory for elicitation (see ``_elicitate``).
        It covers a case when the path is the only elicitation field. The idea is
        to set the path initially to the externally provided value, then, for all
        subsequent elicitations, take it from the previous elicitation data.
        """

        def response_type_factory(data=None):
            nonlocal path
            if data is not None:
                path = data.file_path

            class FileElicitation(BaseModel):
                file_path: str = Field(default=path)

            return path, FileElicitation

        return response_type_factory

    def list_directories(self, directory: OptionalDirectoryArg) -> list[str]:
        """
        Lists subdirectories at the given directory. The directory path is relative
        to the root location.
        """
        return self._list_items(directory, lambda pth: pth.is_dir())

    def list_files(self, directory: OptionalDirectoryArg) -> list[str]:
        """
        Lists files at the given directory. The directory path is relative to the
        root location.
        """
        return self._list_items(directory, lambda pth: pth.is_file())

    def find_files(
        self,
        keywords: Annotated[
            list[str],
            Field(
                description=(
                    "List of keywords to look for in the file path. "
                    "The list should include common inflections of each keyword."
                )
            ),
        ],
        directory: OptionalDirectoryArg,
    ) -> list[str]:
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
        content = keyword_filter(content, keywords, language=self.config.language)
        return [item[PATH_FIELD] for item in content]

    def read_file(self, path: FileArg) -> str:
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
        path: NewFileArg,
        content: Annotated[str, Field(description="File textual content")],
        ctx: Context,
    ) -> None | InputRequiredResult:
        """
        Writes a piece of text to a file at the provided path in BucketFS.
        The path is relative to the root location. An existing file will be overwritten.
        Elicitation is required. If the path is modified in elicitation and there is an
        existing file at the modified path, the elicitation is repeated, to get an
        explicit confirmation that the existing file can be deleted. Writing a file
        in place of existing directory is not allowed.
        """

        def response_type_factory(data=None):
            """
            Similar function to what is created by ``_create_response_type_factory``
            but with added content field.
            """
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
        if isinstance(answer, InputRequiredResult):
            return answer
        abs_path = self.bfs_location.joinpath(answer.file_path)
        byte_content = answer.file_content.encode(encoding="utf-8")
        abs_path.write(byte_content)

    async def download_file(
        self,
        path: NewFileArg,
        url: Annotated[
            str, Field(description="URL where the file should be downloaded from")
        ],
        ctx: Context,
    ) -> None | InputRequiredResult:
        """
        Downloads a file from a given url and writes to a file at the provided path in
        BucketFS. The path is relative to the root location. The same rules apply in
        regard to the path as in the ``write_text_to_file`` tool.
        """

        response_type_factory = self._create_response_type_factory(path)
        message = (
            f"The file at {url} will be downloaded and saved in a BucketFS file "
            "at the give path. The path can be changed if need. Please accept or "
            "decline the operation."
        )

        answer = await self._elicitate(message, ctx, response_type_factory)
        if isinstance(answer, InputRequiredResult):
            return answer
        abs_path = self.bfs_location.joinpath(answer.file_path)

        # A file in a temporary directory, rather than a NamedTemporaryFile, can be
        # reopened by name on Windows.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            tmp_file_name = os.path.join(tmp_dir, "download")
            async with AsyncExitStack() as stack:
                client = await stack.enter_async_context(httpx.AsyncClient(timeout=300))
                response = await stack.enter_async_context(client.stream("GET", url))
                response.raise_for_status()

                # Download in chunks
                afp = await stack.enter_async_context(async_open(tmp_file_name, "wb"))
                async for chunk in response.aiter_bytes(262144):
                    await afp.write(chunk)

            # At the moment, BucketFS only supports synchronous I/O
            def upload_to_bucketfs():
                with open(tmp_file_name, "rb") as f:
                    abs_path.write(f)

            await asyncio.to_thread(upload_to_bucketfs)

    async def delete_file(
        self, path: FileArg, ctx: Context
    ) -> None | InputRequiredResult:
        """
        Deletes a BucketFS file at the specified path.
        """
        response_type_factory = self._create_response_type_factory(path)
        message = (
            "A BucketFS file at the given path is going to be deleted! "
            "Please accept or decline the operation."
        )

        answer = await self._elicitate(
            message, ctx, response_type_factory, expected_status=PathStatus.FileExists
        )
        if isinstance(answer, InputRequiredResult):
            return answer
        abs_path = self.bfs_location.joinpath(answer.file_path)
        abs_path.rm()

    async def delete_directory(
        self, path: DirectoryArg, ctx: Context
    ) -> None | InputRequiredResult:
        """
        Deletes a BucketFS directory at the specified path.
        """
        response_type_factory = self._create_response_type_factory(path)
        message = (
            "A BucketFS directory at the given path is going to be deleted! "
            "Please accept or decline the operation."
        )

        answer = await self._elicitate(
            message, ctx, response_type_factory, expected_status=PathStatus.DirExists
        )
        if isinstance(answer, InputRequiredResult):
            return answer
        abs_path = self.bfs_location.joinpath(answer.file_path)
        abs_path.rmdir(recursive=True)

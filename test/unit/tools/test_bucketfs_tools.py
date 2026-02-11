import pytest

from exasol.ai.mcp.server.tools.bucketfs_tools import (
    PATH_WARNINGS,
    PathStatus,
    get_path_warning,
)


@pytest.mark.parametrize(
    ["status", "expected_status", "expected_warning"],
    [
        (PathStatus.Vacant, None, ""),
        (PathStatus.FileExists, None, PATH_WARNINGS[PathStatus.FileExists]),
        (PathStatus.DirExists, None, PATH_WARNINGS[PathStatus.DirExists]),
        (PathStatus.Invalid, None, PATH_WARNINGS[PathStatus.Invalid]),
        (PathStatus.Vacant, PathStatus.FileExists, PATH_WARNINGS[PathStatus.Vacant]),
        (PathStatus.Vacant, PathStatus.DirExists, PATH_WARNINGS[PathStatus.Vacant]),
        (PathStatus.FileExists, PathStatus.FileExists, ""),
        (
            PathStatus.FileExists,
            PathStatus.DirExists,
            PATH_WARNINGS[PathStatus.FileExists],
        ),
        (
            PathStatus.DirExists,
            PathStatus.FileExists,
            PATH_WARNINGS[PathStatus.DirExists],
        ),
        (PathStatus.DirExists, PathStatus.DirExists, ""),
        (PathStatus.Invalid, PathStatus.FileExists, PATH_WARNINGS[PathStatus.Invalid]),
        (PathStatus.Invalid, PathStatus.DirExists, PATH_WARNINGS[PathStatus.Invalid]),
    ],
    ids=lambda p: p.name if isinstance(p, PathStatus) else "",
)
def test_get_path_warning(status, expected_status, expected_warning) -> None:
    assert get_path_warning(status, expected_status) == expected_warning

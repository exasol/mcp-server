import pytest

from exasol.ai.mcp.server.tools.bucketfs_tools import (
    PATH_WARNINGS,
    PathStatus,
    check_answer,
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


@pytest.mark.parametrize(
    ["path_changed", "status", "expected_status", "expected_result"],
    [
        (False, PathStatus.Vacant, None, True),
        (True, PathStatus.Vacant, None, True),
        (False, PathStatus.FileExists, None, True),
        (True, PathStatus.FileExists, None, False),
        (False, PathStatus.DirExists, None, False),
        (True, PathStatus.DirExists, None, False),
        (False, PathStatus.Invalid, None, False),
        (True, PathStatus.Invalid, None, False),
        (False, PathStatus.Vacant, PathStatus.FileExists, False),
        (True, PathStatus.Vacant, PathStatus.FileExists, False),
        (False, PathStatus.FileExists, PathStatus.FileExists, True),
        (True, PathStatus.FileExists, PathStatus.FileExists, True),
        (False, PathStatus.DirExists, PathStatus.FileExists, False),
        (True, PathStatus.DirExists, PathStatus.FileExists, False),
        (False, PathStatus.Invalid, PathStatus.FileExists, False),
        (False, PathStatus.Vacant, PathStatus.DirExists, False),
        (True, PathStatus.Vacant, PathStatus.DirExists, False),
        (False, PathStatus.FileExists, PathStatus.DirExists, False),
        (True, PathStatus.FileExists, PathStatus.DirExists, False),
        (False, PathStatus.DirExists, PathStatus.DirExists, True),
        (True, PathStatus.DirExists, PathStatus.DirExists, True),
        (False, PathStatus.Invalid, PathStatus.DirExists, False),
    ],
    ids=lambda p: (
        p.name
        if isinstance(p, PathStatus)
        else {True: "changed", False: "unchanged"}.get(p, "")
    ),
)
def test_check_answer(path_changed, status, expected_status, expected_result) -> None:
    prev_path = "dir/file.txt"
    accepted_path = "dir/other.txt" if path_changed else prev_path
    assert (
        check_answer(accepted_path, prev_path, status, expected_status)
        == expected_result
    )

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import exasol.bucketfs as bfs
import pytest

from exasol.ai.mcp.server.setup.server_settings import McpServerSettings
from exasol.ai.mcp.server.tools.bucketfs_tools import (
    PATH_WARNINGS,
    BucketFsTools,
    PathStatus,
    check_answer,
    get_path_warning,
    normalize_path,
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


@pytest.mark.parametrize(
    ["path", "allow_base_dir", "expected_path"],
    [
        ("a/b.txt", True, "a/b.txt"),
        ("a/./b.txt", True, "a/b.txt"),
        ("a//b.txt", True, "a/b.txt"),
        ("a/b/", True, "a/b"),
        ("a/c/../b.txt", True, "a/b.txt"),
        ("", True, ""),
        (".", True, ""),
        ("a/..", True, ""),
        ("a/b.txt", False, "a/b.txt"),
    ],
)
def test_normalize_path(path, allow_base_dir, expected_path) -> None:
    assert normalize_path(path, allow_base_dir) == expected_path


@pytest.mark.parametrize(
    ["path", "allow_base_dir"],
    [
        ("/", True),
        ("/a/b.txt", True),
        ("..", True),
        ("../a/b.txt", True),
        ("a/../../b.txt", True),
        ("a/../..", True),
        ("", False),
        (".", False),
        ("a/..", False),
    ],
)
def test_normalize_path_error(path, allow_base_dir) -> None:
    with pytest.raises(ValueError):
        normalize_path(path, allow_base_dir)


@pytest.fixture
def bucket_dir(tmp_path) -> Path:
    """
    Creates a bucket in a local directory, with one file inside the base directory
    of the tools - root/a/inside.txt, and one file outside it - outside/outside.txt.
    """
    (tmp_path / "root" / "a").mkdir(parents=True)
    (tmp_path / "root" / "a" / "inside.txt").write_text("INSIDE")
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "outside.txt").write_text("OUTSIDE")
    return tmp_path


@pytest.fixture
def bfs_tools(bucket_dir) -> BucketFsTools:
    bucket_root = bfs.path.build_path(backend="mounted", base_path=str(bucket_dir))
    config = McpServerSettings(enable_write_bucketfs=True, disable_elicitation=True)
    return BucketFsTools(bucket_root.joinpath("root"), config)


def _assert_outside_intact(bucket_dir: Path) -> None:
    assert (bucket_dir / "outside" / "outside.txt").read_text() == "OUTSIDE"


def test_read_file_inside_base_dir(bfs_tools) -> None:
    assert bfs_tools.read_file("a/inside.txt") == "INSIDE"
    assert bfs_tools.read_file("a/../a/inside.txt") == "INSIDE"


@pytest.mark.parametrize(
    "path", ["/outside/outside.txt", "../outside/outside.txt", "a/../../outside.txt"]
)
def test_read_file_outside_base_dir(bfs_tools, path) -> None:
    with pytest.raises(ValueError):
        bfs_tools.read_file(path)


@pytest.mark.parametrize("path", ["/", "/outside", "..", "../outside"])
def test_list_outside_base_dir(bfs_tools, path) -> None:
    with pytest.raises(ValueError):
        bfs_tools.list_files(path)
    with pytest.raises(ValueError):
        bfs_tools.list_directories(path)
    with pytest.raises(ValueError):
        bfs_tools.find_files(["outside"], path)


@pytest.mark.parametrize(
    "path",
    [
        "/outside/outside.txt",
        "../outside/outside.txt",
        "a/../../outside/outside.txt",
        "",
        "a/..",
    ],
)
def test_get_path_status_invalid(bfs_tools, path) -> None:
    assert bfs_tools._get_path_status(path) == PathStatus.Invalid


@pytest.mark.parametrize(
    ["path", "expected_status"],
    [
        ("a/inside.txt", PathStatus.FileExists),
        ("a/./inside.txt", PathStatus.FileExists),
        ("a", PathStatus.DirExists),
        ("a/new.txt", PathStatus.Vacant),
    ],
)
def test_get_path_status_valid(bfs_tools, path, expected_status) -> None:
    assert bfs_tools._get_path_status(path) == expected_status


@pytest.mark.parametrize(
    "path",
    [
        "/outside/outside.txt",
        "../outside/outside.txt",
        "a/../../outside/outside.txt",
        "",
    ],
)
def test_write_text_to_file_outside_base_dir(bfs_tools, bucket_dir, path) -> None:
    coro = bfs_tools.write_text_to_file(path, "OVERWRITTEN", MagicMock())
    with pytest.raises(ValueError):
        asyncio.run(coro)
    _assert_outside_intact(bucket_dir)


def test_write_text_to_file_inside_base_dir(bfs_tools, bucket_dir) -> None:
    asyncio.run(bfs_tools.write_text_to_file("a/b/../new.txt", "NEW", MagicMock()))
    assert (bucket_dir / "root" / "a" / "new.txt").read_text() == "NEW"


@pytest.mark.parametrize("path", ["/outside/outside.txt", "../outside/outside.txt", ""])
def test_delete_file_outside_base_dir(bfs_tools, bucket_dir, path) -> None:
    coro = bfs_tools.delete_file(path, MagicMock())
    with pytest.raises(ValueError):
        asyncio.run(coro)
    _assert_outside_intact(bucket_dir)


@pytest.mark.parametrize("path", ["/", "/outside", "../outside", "..", "", "a/.."])
def test_delete_directory_outside_base_dir(bfs_tools, bucket_dir, path) -> None:
    coro = bfs_tools.delete_directory(path, MagicMock())
    with pytest.raises(ValueError):
        asyncio.run(coro)
    _assert_outside_intact(bucket_dir)
    assert (bucket_dir / "root" / "a" / "inside.txt").read_text() == "INSIDE"

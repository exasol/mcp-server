import multiprocessing

from _pytest.monkeypatch import MonkeyPatch


def use_fork_start_method(monkeypatch: MonkeyPatch) -> None:
    """
    Since Python 3.14 the default multiprocessing start method on POSIX is
    "forkserver", which requires the ``multiprocessing.Process`` target and its
    arguments to be picklable. Patches ``multiprocessing.Process`` to use the
    "fork" context instead, scoped to the calling test via ``monkeypatch``.
    """
    monkeypatch.setattr(
        multiprocessing, "Process", multiprocessing.get_context("fork").Process
    )

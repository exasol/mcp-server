from exasol.ai.mcp.server.utils.named_object_pool import NamedObjectPool


class SimpleTestClass:
    def __init__(self, state: int) -> None:
        self.state = state

    def cleanup(self):
        self.state = -1


def test_named_object_pool():
    pool: NamedObjectPool[SimpleTestClass] = NamedObjectPool(
        capacity=3, cleanup=lambda o: o.cleanup()
    )
    d: dict[str, SimpleTestClass] = {}
    for c in "abcd":
        obj = pool.checkout(c)
        assert obj is None
        obj = SimpleTestClass(1)
        pool.checkin(c, obj)
        d[c] = obj

    # Object "a" should be kicked out and cleaned up
    assert pool.checkout("a") is None
    assert d["a"].state < 0

    # "b" should still be there
    obj_b = pool.checkout("b")
    assert obj_b is not None
    assert obj_b.state > 0
    pool.checkin("b", obj_b)

    # When the next object is added, it's "c" that should be evicted, not "b" because
    # "b" has been added back in.
    pool.checkin("e", SimpleTestClass(1))
    assert pool.checkout("c") is None
    assert obj_b.state > 0


def test_named_object_pool_checkin_on_eviction():
    pool: NamedObjectPool[SimpleTestClass] = NamedObjectPool(
        capacity=1, cleanup=lambda o: o.cleanup()
    )
    obj_a, obj_b = SimpleTestClass(1), SimpleTestClass(2)
    pool.checkin("a", obj_a)
    pool.checkin("b", obj_b)
    # "a" should be evicted and cleaned up, while "b" is stored intact.
    assert obj_a.state < 0
    assert pool.checkout("a") is None
    assert pool.checkout("b") is obj_b
    assert obj_b.state == 2


def test_named_object_pool_checkin_same_name():
    pool: NamedObjectPool[SimpleTestClass] = NamedObjectPool(
        capacity=3, cleanup=lambda o: o.cleanup()
    )
    obj_old, obj_new = SimpleTestClass(1), SimpleTestClass(2)
    pool.checkin("a", obj_old)
    pool.checkin("a", obj_new)
    # The old object should be replaced and cleaned up.
    assert obj_old.state < 0
    assert pool.checkout("a") is obj_new
    assert obj_new.state == 2


def test_named_object_pool_cleanup_error():
    def failing_cleanup(o: SimpleTestClass) -> None:
        raise RuntimeError("cleanup failed")

    pool: NamedObjectPool[SimpleTestClass] = NamedObjectPool(
        capacity=1, cleanup=failing_cleanup
    )
    obj_b = SimpleTestClass(2)
    pool.checkin("a", SimpleTestClass(1))
    # A failure to clean up the evicted object should not affect the checkin.
    pool.checkin("b", obj_b)
    assert pool.checkout("a") is None
    assert pool.checkout("b") is obj_b

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

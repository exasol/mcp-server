import threading
from collections import OrderedDict
from collections.abc import Callable
from typing import (
    Generic,
    TypeVar,
)

T = TypeVar("T")


class NamedObjectPool(Generic[T]):
    """
    A simple implementation of a thread-safe named object pool.

    There can be only one object in the pool with a given name. The pool has limited
    capacity. When it is exceeded, the least recently accessed object gets evicted.

    An object should not be shared between multiple threads. However, the class itself
    cannot enforce this. The consumer is supposed to return the object to the pool only
    when it has completely done with it.
    """

    def __init__(self, capacity: int, cleanup: Callable[[T], None] | None = None):
        self.capacity = capacity
        self.cleanup = cleanup
        self._available: OrderedDict[str, T] = OrderedDict()
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)

    def checkout(self, name: str) -> T | None:
        """
        Checks out an object by name, returns None if not available.
        """
        with self._condition:
            if name in self._available:
                return self._available.pop(name)
        return None

    def checkin(self, name: str, obj: T) -> None:
        """
        Returns an object to the pool.
        May need to evict the least recently added object from there to free the space.
        """
        with self._condition:
            if name in self._available:
                obj = self._available.pop(name)
                if self.cleanup is not None:
                    self.cleanup(obj)
            while len(self._available) >= self.capacity:
                _, obj = self._available.popitem(last=False)
                if self.cleanup is not None:
                    self.cleanup(obj)
            self._available[name] = obj

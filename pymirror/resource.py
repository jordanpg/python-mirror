from dataclasses import dataclass
from heapq import heapify, heappop, heappush
from pymirror.process import Process


@dataclass
class Lock:
    """A lock provides restricted access to a copy of a resource"""
    holder: Process = None

class Resource:
    """A Resource models all copies of a resource across all sites, represented as a set of locks"""
    def __init__(self, id: int, num_copies: int) -> None:
        self.id = id
        self.copies = frozenset([Lock() for _ in range(num_copies)])
        self.queue: list[tuple[int, Process]] = [] # Blocking queue
        
    def acquire(self, p: Process) -> bool:
        """Acquire a lock to this resource or enter queue. Returns True if a lock is acquired immediately."""
        for c in self.copies:
            if c.holder is None:
                p.lock = c
                c.holder = p
                return True
        heappush(self.queue, (p.priority, p))
        return False
    
    def release(self, p: Process):
        """Release a lock held by this process or exit queue"""
        for c in self.copies:
            if c.holder is p:
                p.lock = None
                # If the queue is empty, then just leave the lock free.
                if len(self.queue) < 1:
                    c.holder = None
                    return
                # Pass the lock to the next process in the queue
                _, np = heappop(self.queue)
                np.lock = c
                c.holder = np
                return
        # Remove this process from the queue
        self.queue.remove((p.priority, p))
        heapify(self.queue)
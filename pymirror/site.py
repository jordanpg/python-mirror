from pymirror.mirror import Mirror
from pymirror.work import Cohort, Page

class Lock:
    def __init__(self, sim: Mirror):
        self.holder: Cohort = None
        self.queue: list[tuple[int, Cohort]] = None
        
    def acquire(self, req: Cohort):
        pass

class Site:
    """Represents a single site in a distributed database system"""
    def __init__(self, sim: Mirror):
        self.options = sim.options
        self.queue: list[tuple[float, Cohort]] = []
        self.pages: list[Page] = []
        self.locks: dict[Page, Lock] = {}
from dataclasses import dataclass
from enum import Enum
from time import time_ns
from typing import Iterable
from pymirror.mirror import Mirror
from pymirror.site import Site

class Page:
    """Models a data object"""
    def __init__(self, id: int, site: Site = None, replicas: Iterable['Page'] = None) -> None:
        self.id=id
        self.last_modified = 0
        self.site = site
        self.replicas: set[Page] = set(replicas) if replicas else set()
        self.replicas.add(self)
        
    def replicate(self) -> 'Page':
        rep = Page(self.id, replicas=self.replicas)
        for i in self.replicas:
            i.replicas.add(rep)
        return rep
    
    def modify(self):
        self.last_modified = time_ns()
    
class CohortState(Enum):
    Read=0
    Prepare=1
    Prepared=2
    Commit=3
    Abort=4

class Cohort:
    """Models a single process involved in a transaction"""
    def __init__(self, master: 'Transaction', time: int, page: Page) -> None:
        self.master = master
        self.progress = 0
        self.length = time
        self.state = CohortState.Read
        self.page = page
        
    def tick(self):
        self.progress += 1
    
    def restart(self):
        self.progress = 0
        self.state = CohortState.Read
        
    

class Transaction:
    """Simulated transaction"""
    def __init__(self, sim: Mirror) -> None:
        self.sim = sim
        self.cohorts: list[Cohort] = []
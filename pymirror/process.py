from enum import Enum
from xmlrpc.client import boolean
from pymirror.resource import Lock, Resource
from pymirror.transaction import Transaction


class ProcessType(Enum):
    Worker=0b001 # A worker will finish its progress then enter Contract state, and then complete if it is not a writer.
    Writer=0b011 # A writer will spawn updaters for all other copies once entering the Contract state, then complete when each updater is ready.
    Updater=0b100 # An updater will acquire a lock and then enter Contract state, and complete when signaled by the writer.

class ProcessState(Enum):
    Begin=0     # Initial state, before the process arrives at the resource
    Expand=1    # Waiting/working state
    Contract=2  # Work finished, ready to commit or complete

class Process:
    """A process models a working unit of a transaction"""
    def __init__(self, owner: Transaction, target: Resource, length: int, type: ProcessType) -> None:
        self.owner = owner
        self.length = length
        self.progress = 0 # Number of ticks spent on current task
        self.type = type
        self.state = ProcessState.Begin
        self.target = target
        self.updaters: list[Process] = []
        self.lock: Lock = None
        
    def tick(self) -> bool:
        """Progress this process. Returns True if the process has completed."""
        if self.state is not ProcessState.Contract:
            # Register with resource 
            if self.state is ProcessState.Begin:
                self.target.acquire(self)
                self.state = ProcessState.Expand
            
            # If we aren't blocking, then do some work
            if self.lock is not None:
                self.progress += 1
                if self.progress >= self.length:
                    return self.ready()
        else:
            pass
        return False
                
    def ready(self):
        """Ready this process and do any subsequent work. Returns True if the process has completed."""
        self.state = ProcessState.Contract
        
        if self.type is ProcessType.Worker:
            return self.complete()
        
        self.progress = 0
        return False
    
    def complete(self):
        """Complete and release any locks held by this process or its updaters"""
        self.release_all()
        return True
    
    def release_all(self):
        """Release any held locks by this process or its updaters and exit blocking queues"""
        self.target.release(self)
        for u in self.updaters:
            self.target.release(u)
    
    @property
    def priority(self):
        return self.owner.deadline
    
    @property
    def is_worker(self):
        return bool(self.type.value & ProcessType.Worker.value)
    
    @property
    def is_writer(self):
        return self.type == ProcessType.Writer
    
    @property
    def is_updater(self):
        return self.type == ProcessType.Updater
    
    @property
    def spawning(self):
        if not self.is_writer:
            return False
        if self.state is not ProcessState.Contract:
            return False
        return len(self.updaters) < (len(self.target.copies) - 1)
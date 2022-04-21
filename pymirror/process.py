from enum import Enum
from xmlrpc.client import boolean
from pymirror.resource import Lock, Resource
from pymirror.transaction import Transaction


class ProcessType(Enum):
    Worker=0b001 # A worker will finish its progress then enter Contract state, and then complete if it is not a writer.
    Writer=0b011 # A writer will spawn updaters for all other copies once entering the Contract state, then complete when each updater is ready.
    Updater=0b100 # An updater will acquire a lock, initiate a write, and then enter Contract state, and complete when signaled by the writer.

class ProcessState(Enum):
    Begin=0     # Initial state, before the process arrives at the resource
    Expand=1    # Waiting/working state
    Contract=2  # Work finished, ready to commit or complete
    Complete=3  # Process is done.

class Process:
    """A process models a working unit of a transaction"""
    def __init__(self, owner: Transaction, target: Resource, length: int, type: ProcessType) -> None:
        self.owner = owner
        self.length = length
        self.add_length = 0 # Amount of additional work incurred during execution.
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
                self.target.acquire(self) # Request a lock on the resource
                self.state = ProcessState.Expand
            
            # If we aren't blocking, then do some work
            if self.lock is not None:
                self.progress += 1
                if self.progress >= self.length:
                    return self.ready()
        else:
            # If this process is currently spawning updaters, increase progress.
            if self.spawning:
                self.progress += 1
                # If we should create another updater, create it and then start working on the next if needed.
                if self.progress >= self.length:
                    self.spawn_updater()
                    # If we still need more updaters, increase length again.
                    if self.spawning:
                        self.length += self.owner.sim.options.spawn_time
                    return False
            elif self.is_writer:
                # If all updaters are finish
                for i in self.updaters:
                    if i.state is ProcessState.Contract:
                        return False
                return self.complete()
                    
        return False
                
    def ready(self):
        """Ready this process and do any subsequent work. Returns True if the process has completed."""
        self.state = ProcessState.Contract
        
        # If we are just a worker, then we no changes to commit and can simply release our lock and finish.
        if self.is_worker:
            return self.complete()
        # If we are a writer, then we need to start working on spawning updaters.
        if self.is_writer:
            # If spawn_time is configured as 0 or less, just instantly create all updaters.
            if self.owner.sim.options.spawn_time < 1:
                while self.spawning:
                    self.spawn_updater()
                return False

            self.length += self.owner.sim.options.spawn_time
            
        return False
    
    def spawn_updater(self):
        """Create an updater to acquire a lock for the resource"""
        updater = Process(self.owner, self.target, self.owner.sim.options.write_time, ProcessType.Updater)
        self.updaters.append(updater)
    
    def complete(self):
        """Complete and release any locks held by this process or its updaters. The commit phase is presumed to occur here as well."""
        self.release_all()
        return True
    
    def release_all(self):
        """Release any held locks by this process or its updaters and exit blocking queues"""
        self.target.release(self)
        for u in self.updaters:
            self.target.release(u)
            u.complete()
    
    @property
    def priority(self):
        return self.owner.deadline
    
    @property
    def is_worker(self):
        return self.type == ProcessType.Worker
    
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
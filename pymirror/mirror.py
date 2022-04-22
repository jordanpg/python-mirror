from dataclasses import dataclass
from enum import Enum
from heapq import heapify, heappop, heappush
import math
import random
from tqdm import tqdm
from typing import Union

def poisson(lmb, k):
    return lmb ** k * math.exp(-lmb) / math.factorial(k)

@dataclass
class MirrorOptions:
    # Simulation structure settings
    db_size = 1000  # Number of resources to generate
    replicas = 4    # Number of replicas for each resource
    cpu_count = 8   # Number of CPUs in the system
    sim_size = 1000  # Number of transactions to complete

    # Timing settings
    access_time = 20        # Number of ticks to access a data item on disk (assumed to be the case)
    buffered_time = 10      # Number of ticks to access a data item in the buffer pool
    buffered_chance = 0.1   # Probability of using buffered_time instead of access_time
    write_time = 2          # Number of ticks to initiate a write
    spawn_time = 1          # Number of ticks to spawn a remote updater

    # Transaction settings
    arrival_rate = 16           # Number of transactions expected per 1000 ticks
    deadline_slack = 6          # Multiplier for expected transaction length
    transaction_size = (5,16)   # Range of possible transaction sizes (i.e., number of processes spawned)
    write_chance = 0.25         # Probability that a spawned process will require write access

class Mirror:
    """Coordinator for the MIRROR simulation"""
    default_options = MirrorOptions()
    @staticmethod
    def pa_pb(holder: 'Process', requestor: 'Process') -> bool:
        """Defines the PA_PB process used to solve lock conflicts. Returns True if holder should be aborted."""
        # For updaters, block if the process has acquired all their local locks
        if holder.is_updater:
            if holder.lock is None:
                return holder.priority < requestor.priority
            return False
        # For workers, block if the process has entered the Contract state
        if holder.state >= 2:
            return False
        
        # Otherwise, use priority abort and abort the holder if its priority is lower.
        return holder.priority < requestor.priority
        
    def __init__(self, options: MirrorOptions = default_options) -> None:
        self.options = options
        self.clock = 0
        self.finished = 0
        self.missed = 0
        self.started = 0
        self.transactions: list[Transaction] = []
        self.cpus: list[Cpu] = []
        self.resources: list[Resource] = []
        self.processes: list[tuple[int, Process]] = []
        self.method = Mirror.pa_pb
        
        self._badtick = 0
        self._chance = poisson(options.arrival_rate / 1000, 1) # Calculate probability of transcation arrival each tick
        self._pbar: tqdm = None
    
    def start_sim(self):
        ops = self.options
        self.cpus = [Cpu() for _ in range(ops.cpu_count)] # Initialize CPUs
        self.resources = [Resource(i, ops.replicas) for i in range(ops.db_size)] # Initialize resources
        
        with tqdm(total=ops.sim_size,postfix={'Finished':0,'Missed':0,'Clock':0,'Working':0}) as self._pbar:
            while (self.finished + self.missed) < ops.sim_size:
                self.tick()
            
        print(f"Finished: {self.finished}/{ops.sim_size}, ({self.finished / ops.sim_size}%)")
        print(f"Missed: {self.missed}/{ops.sim_size}, ({self.missed / ops.sim_size}%)")
        print(f"Bad ticks: {self._badtick}/{self.clock}, ({self._badtick/self.clock}%)")
    
    def tick(self):
        """Progress the entire simulation by one step"""
        self.clock += 1
        # Kill expired transactions
        for t in self.transactions:
            if self.clock > t.deadline:
                t.abort()
                self.transaction_finish(t, miss=True)
                
        # Determine if we should spawn a new process
        if random.random() < self._chance:
            self.create_transaction()
                
        # Advance running processes
        proc = self.pick_processes(self.options.cpu_count)
        for p in proc:
            p.tick()
        if len(proc) == 0:
            self._badtick += 1
        
        # Update progress bar occasionally
        if self._pbar and self.clock % 100 == 0:
            self._pbar.set_postfix({'Finished':self.finished,'Missed':self.missed,'Clock':self.clock})
            
    def submit_job(self, p: 'Process') -> 'Cpu':
        """Schedule a process"""
        heappush(self.processes, (p.priority, p))
    
    def remove_job(self, p: 'Process'):
        """Remove a process from the schedule, releasing any locks it is holding"""
        p.release_all()
        j = (p.priority,p)
        if j in self.processes:
            self.processes.remove(j)
            heapify(self.processes)
            
    def transaction_finish(self, t: 'Transaction', miss = False):
        """Signal to the simulation that a transaction has completed"""
        if t not in self.transactions:
            return
        if miss:
            self.missed += 1
        else:
            self.finished += 1
        if self._pbar:
            self._pbar.set_postfix({'Finished':self.finished,'Missed':self.missed,'Clock':self.clock},refresh=False)
            self._pbar.update()
        self.transactions.remove(t)
        
    def create_transaction(self) -> 'Transaction':
        t = Transaction(self)
        self.transactions.append(t)
        self.started += 1
        t.begin()
        # print(self.processes)
        return t
    
    def pick_processes(self, num: int):
        """Determine which processes should be running"""
        chosen: list[Process] = []
        for (_,p) in self.processes:
            if p.state == 0: # Always tick a process if it is still in the Begin phase
                chosen.append(p)
                continue
            elif p.blocking: # Skip processes that are blocking
                continue
            chosen.append(p)
        return chosen[:num]

class Transaction:
    """A transaction models a database transaction, spawning processes and awaiting their completion"""
    def __init__(self, sim: Mirror) -> None:
        self.sim = sim
        self.processes: list[Process] = []
        self.dependencies: list[Resource] = []
        self.arrival = sim.clock
        self.deadline = -1
        
    def begin(self):
        ops = self.sim.options
        self.deadline = self.arrival
        size = random.randint(*ops.transaction_size)
        # Pick a collection of random without replacement
        resources = random.sample(self.sim.resources, size)
        self.dependencies = resources
        self.spawn_processes()
        # Extend deadline based on expected length of each process
        # i.e., calculate expected length with no buffer or replication
        self.deadline += sum([ops.access_time + (ops.write_time if p.is_writer else 0) for p in self.processes]) * ops.deadline_slack
    
    def spawn_processes(self):
        ops = self.sim.options
        resources = self.dependencies
        # Create processes for each resource we want to access
        for r in resources:
            plen = ops.access_time if random.random() > ops.buffered_chance else ops.buffered_time
            # Add write time for writer processes
            writer = random.random() < ops.write_chance
            if writer:
                plen += ops.write_time
            # Create and submit the process
            p = Process(self, r, plen, 1 if writer else 0)
            self.processes.append(p)
            self.sim.submit_job(p)
    
    def abort(self):
        """Remove all this transaction's jobs from the simulation"""
        # Remove all our jobs from the simulation
        for p in self.processes:
            self.sim.remove_job(p)
        self.processes = []
        
    def restart(self):
        """Abort and then restart if there is time"""
        self.abort()
        # If we still have time, restart.
        if self.sim.clock <= self.deadline:
            self.spawn_processes()
    
    def commit(self, p: 'Process'):
        """Notify this transaction that a process is finished, then complete if all processes are finished"""
        if self.sim.clock > self.deadline:
            self.sim.transaction_finish(self, miss=True)
            return
        
        if p not in self.processes:
            return
        
        self.sim.remove_job(p)
        
        if all([i.state == 3 for i in self.processes]):
            self.sim.transaction_finish(self)

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
    def __init__(self, owner: Transaction, target: 'Resource', length: int, type: ProcessType) -> None:
        self.owner = owner
        self.arrival = owner.sim.clock
        self.length = length
        self.add_length = 0 # Amount of additional work incurred during execution.
        self.progress = 0 # Number of ticks spent on current task
        self.type = type
        self.state = 0
        self.target = target
        self.updaters: list[Process] = []
        self.lock: Union[Lock, None] = None
        self.last_tick = 0
        
    def tick(self) -> bool:
        """Progress this process. Returns True if the process has completed."""
        if self.state < 2:
            # Register with resource 
            if self.state == 0:
                self.target.acquire(self) # Request a lock on the resource
                self.state = 1
            
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
                # If all updaters are finished
                for i in self.updaters:
                    if i.state < 2:
                        return False
                return self.complete()
                    
        return False
                
    def ready(self):
        """Ready this process and do any subsequent work. Returns True if the process has completed."""
        self.state = 2
        
        # If we are just a worker, then we no changes to commit and can simply release our lock and finish.
        if self.is_worker:
            return self.complete()
        # If we are a writer, then we need to start working on spawning updaters.
        if self.is_writer:
            # If we don't need to spawn any updaters, then finish here.
            if not self.spawning:
                return self.complete()
            # If spawn_time is configured as 0 or less, just instantly create all updaters.
            if self.owner.sim.options.spawn_time < 1:
                while self.spawning:
                    self.spawn_updater()
                return False

            self.length += self.owner.sim.options.spawn_time
            
        return False
    
    def spawn_updater(self):
        """Create an updater to acquire a lock for the resource"""
        updater = Process(self.owner, self.target, self.owner.sim.options.write_time, 2)
        self.updaters.append(updater)
        self.owner.sim.submit_job(updater)
    
    def complete(self):
        """Complete and release any locks held by this process or its updaters. The commit phase is presumed to occur here as well."""
        self.state = 3
        self.release_all()
        self.owner.commit(self)
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
        return self.type == 0
    
    @property
    def is_writer(self):
        return self.type == 1
    
    @property
    def is_updater(self):
        return self.type == 2
    
    @property
    def spawning(self):
        if not self.is_writer:
            return False
        if self.state != 2:
            return False
        return len(self.updaters) < (self.owner.sim.options.replicas-1)
    
    @property
    def blocking(self):
        """Is True when the process should yield CPU time"""
        if self.state == 3: # Skip finished processes
            return True
        if self.state != 0 and self.lock is None: # Skip processes that are waiting for a lock
            return True
        if self.is_writer and not self.spawning: # Skip processes that are waiting for updaters to finish
            for u in self.updaters:
                if u.state < 2:
                    return True
        if self.is_updater and self.state >= 2: # Skip readied updaters
            return True
        return False
    
    def __eq__(self, __o: object) -> bool:
        if not isinstance(__o, Process):
            return False
        if self.priority == __o.priority:
            return self.arrival == __o.arrival
        return self.priority == __o.priority
    
    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, Process):
            return False
        if self.priority == __o.priority:
            if self.arrival == __o.arrival:
                return self.state < __o.state
            return self.arrival < __o.arrival
        return self.priority < __o.priority
    
class Cpu:
    """A CPU propagates ticks to one process at a time"""
    def __init__(self) -> None:
        self.processes: list[tuple[int, Process]] = [] # Initialize heap
        
    def schedule(self, p: Process):
        """Schedule a process to this CPU"""
        # Add this process to the heap, using the transaction deadline as the key value
        heappush(self.processes, (p.priority, p))
        
    def deschedule(self, p: Process):
        j = (p.priority, p)
        if j not in self.processes:
            return
        self.processes.remove((p.priority, p))
        heapify(self.processes)
        
    def tick(self):
        """Propagate tick to running process"""
        if len(self.processes) < 1:
            return

        ind = self.running_index()
        if ind < 0: # All processes are blocking!
            return
        
        # Tick running process, then remove it if it has completed.
        job = self.processes[ind]
        if job[1].tick():
            self.processes.remove(job)
            heapify(self.processes)
            
    def running_index(self):
        """Determine which process should be running"""
        for ind,(_,p) in enumerate(self.processes):
            if p.state == 0: # Always tick a process if it is still in the Begin phase
                return ind
            elif p.blocking: # Skip processes that are blocking
                continue
            return ind
        return -1

@dataclass
class Lock:
    """A lock provides restricted access to a copy of a resource"""
    holder: Union[Process, None] = None

class Resource:
    """A Resource models all copies of a resource across all sites, represented as a set of locks"""
    def __init__(self, id: int, num_copies: int) -> None:
        self.id = id
        self.copies = [Lock() for _ in range(num_copies)]
        self.queue: list[tuple[int, Process]] = [] # Blocking queue
    
    def held_by(self, p: Process):
        for l in self.copies:
            if l.holder is p:
                return True
        return False
    
    def acquire(self, p: Process) -> bool:
        """Acquire a lock to this resource or enter queue. Returns True if a lock is acquired immediately."""
        # See if any locks are open
        for c in self.copies:
            if c.holder is None:
                p.lock = c
                c.holder = p
                return True
        # See if we can use PA to acquire a lock
        for c in self.copies:
            if c.holder.owner.sim.method(c.holder, p):
                c.holder.owner.restart()
                p.lock = c
                c.holder = p
                return True
        # Otherwise, use PB
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
        j = (p.priority, p)
        if j not in self.queue:
            return
        self.queue.remove((p.priority, p))
        heapify(self.queue)
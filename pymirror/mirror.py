from dataclasses import dataclass
from pymirror.cpu import Cpu
from pymirror.transaction import Transaction
from pymirror.resource import Resource

@dataclass
class MirrorOptions:
    # Simulation structure settings
    db_size = 1000  # Number of resources to generate
    replicas = 4    # Number of replicas for each resource
    cpu_count = 8   # Number of CPUs in the system
    
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
    def __init__(self, options: MirrorOptions = default_options) -> None:
        self.options = options
        self.transactions: list[Transaction] = []
        self.cpus = [Cpu() for _ in range(options.cpu_count)] # Initialize CPUs
        self.resources = [Resource(options.replicas) for _ in range(options.db_size)] # Initialize resources
        
    def tick(self):
        """Progress the entire simulation by one step"""
        for cpu in self.cpus:
            cpu.tick()
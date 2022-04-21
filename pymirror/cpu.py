from heapq import heapify, heappop, heappush
from pymirror.process import Process, ProcessState

class Cpu:
    """A CPU propagates ticks to one process at a time"""
    def __init__(self) -> None:
        self.processes: list[tuple[int, Process]] = [] # Initialize heap
        
    def schedule(self, p: Process):
        """Schedule a process to this CPU"""
        # Add this process to the heap, using the transaction deadline as the key value
        heappush(self.processes, (p.priority, p))
        
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
            if p.state is ProcessState.Begin: # Always tick a process if it is still in the Begin phase
                return ind
            elif p.lock is None: # Skip processes that are blocking
                continue
            return ind
        return -1
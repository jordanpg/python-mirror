from pymirror.mirror import Mirror
from pymirror.process import Process


class Transaction:
    """A transaction models a database transaction, spawning processes and awaiting their completion"""
    def __init__(self, sim: Mirror, curr_time: int) -> None:
        self.sim = sim
        self.processes: list[Process] = []
        self.deadline = curr_time
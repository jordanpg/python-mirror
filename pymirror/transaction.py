from pymirror.process import Process


class Transaction:
    """A transaction models a database transaction, spawning processes and awaiting their completion"""
    def __init__(self, curr_time: int) -> None:
        self.processes: list[Process] = []
        self.deadline = curr_time
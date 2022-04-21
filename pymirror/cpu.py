from pymirror.mirror import Mirror

class Cpu:
    """Simulated CPU"""
    def __init__(self, sim: Mirror) -> None:
        self.options = sim.options
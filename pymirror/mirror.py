
from dataclasses import dataclass

from pymirror.work import Cohort

@dataclass
class MirrorOptions:
    """Options for MIRROR simulation"""
    # Simulation structure options
    num_sites=4
    db_size=1000
    repl_degree=4
    # Site properties
    site_cpus=2
    site_data_disks=4
    site_log_disks=1,
    # Transaction behavior options
    buffer_hit_ratio=0.1
    arrival_rate=16
    slack_factor=6.0
    transaction_size=16
    update_frequency=0.25
    # CPU time used for various functions in ns
    page_cpu=10000
    init_write_cpu=2000
    page_disk_cpu=20000
    log_disk_cpu=5000
    message_cpu=1000

class Mirror:
    """MIRROR simulation master"""
    default_options = MirrorOptions()
    def __init__(self, options: MirrorOptions = None):
        self.options = options or self.default_options
        self.last_transaction = 0 # Time when last transaction was created
        
    @staticmethod
    def policy_pa_pb_o2pl(holder: Cohort, requestor: Cohort):
        """Defines PA_PB state-conscious priority blocking"""
        pri_h = holder.master.deadline
        pri_r = requestor.master.deadline
        
        passed_demarc = False
        if holder.is_updater:
            pass
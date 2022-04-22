import csv
from pymirror.mirror import *

def run_test(cpus: int, arrival_rate: int, ops: MirrorOptions = None):
    """Run a MIRROR simulation with a given number of CPUs and arrival rate"""
    if not ops:
        ops = MirrorOptions()
    ops.cpu_count = cpus
    ops.arrival_rate = arrival_rate
    # Run simulation
    mirror = Mirror(ops)
    mirror.start_sim()
    
    return {
        'cpus': cpus,
        'arrival_rate': arrival_rate,
        'cycles': mirror.clock,
        'num_finished': mirror.finished,
        'num_missed': mirror.missed,
        'miss_pct': mirror.missed / ops.sim_size,
        'idle_cycles': mirror._badtick,
        'cc_aborts': mirror.aborts
    }

if __name__ == "__main__":
    cpu_tests = [8, 16, 32]
    arrival_rate_tests = range(5,76,5)
    results = []
    
    ops_template = MirrorOptions()
    
    for cpus in cpu_tests:
        for arrival_rate in arrival_rate_tests:
            print(f'Running test: {cpus} CPUs, {arrival_rate} transactions/1000 cycles')
            results.append(run_test(cpus, arrival_rate, ops_template))
    
    print("Writing results to results.csv...")
    with open("results.csv", 'w') as file:
        fields = ['cpus', 'arrival_rate', 'cycles', 'num_finished', 'num_missed', 'miss_pct', 'idle_cycles', 'cc_aborts']
        dw = csv.DictWriter(file, fieldnames=fields)
        
        dw.writeheader()
        dw.writerows(results)
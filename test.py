from collections import namedtuple
from pymirror.mirror import *

def writer():
    opt = MirrorOptions()
    opt.replicas = 2
    m = Mirror(opt)
    r = m.resources[0]
    c = m.cpus[0]
    t = Transaction(m, 0)
    p = Process(t, r, 10, ProcessType.Writer)
    t.processes.append(p)
    
    c.schedule(p)
    assert p.lock is None
    assert p.state is ProcessState.Begin
    m.tick()
    assert p.lock is r.copies[0]
    assert p.state is ProcessState.Expand
    assert p.progress == 1
    for _ in range(9):
        m.tick()
    assert p.progress == 10
    assert p.spawning
    assert p.state is (ProcessState.Complete if p.type is ProcessType.Worker else ProcessState.Contract)
    assert not p.blocking
    m.tick()
    assert p.blocking
    u = p.updaters[0]
    c.schedule(u)
    assert u.lock is None
    assert any([l.holder is None for l in r.copies])
    m.tick()
    assert u.lock is not None
    assert u.progress == 1
    assert all([l.holder for l in r.copies])
    m.tick()
    assert u.state == ProcessState.Contract
    m.tick()
    assert u.state == ProcessState.Complete
    assert p.state == ProcessState.Complete

if __name__ == "__main__":
    # writer()
    o = MirrorOptions()
    # o.write_chance = 1
    # o.sim_size = 100
    o.arrival_rate = 50
    m = Mirror(o)
    m.start_sim()
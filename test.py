from collections import namedtuple
from pymirror.mirror import *

def writer():
    opt = MirrorOptions()
    opt.replicas = 2
    opt.cpu_count = 1
    m = Mirror(opt)
    m.initialize()
    r = m.resources[0]
    t = Transaction(m)
    p = Process(t, r, 10, 1)
    t.processes.append(p)
    
    m.submit_job(p)
    assert p.lock is None
    assert p.state == 0
    m.tick()
    assert p.lock is r.copies[0]
    assert p.state == 1
    assert p.progress == 1
    for _ in range(9):
        m.tick()
    assert p.progress == 10
    assert p.spawning
    assert p.state == (3 if p.type == 0 else 2)
    assert not p.blocking
    m.tick()
    assert len(p.updaters) > 0
    assert p.blocking
    u = p.updaters[0]
    assert u.lock is None
    assert any([l.holder is None for l in r.copies])
    m.tick()
    assert u.lock is not None
    assert u.progress == 1
    assert all([l.holder for l in r.copies])
    m.tick()
    assert u.state == 2
    m.tick()
    assert u.state == 3
    assert p.state == 3

if __name__ == "__main__":
    # writer()
    o = MirrorOptions()
    o.replicas = 4
    o.cpu_count = 64
    # o.write_chance = 1
    # o.sim_size = 100
    o.arrival_rate = 100
    m = Mirror(o)
    m.start_sim()
import daemon
from Scheduler import Scheduler

with daemon.DaemonContext():
    Scheduler()
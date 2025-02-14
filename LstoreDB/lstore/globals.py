from threading import BoundedSemaphore
from lstore.disk_manager import DiskManager
from lstore.lock_manager import LockManager
from lstore.fakeLockManager import FakeLockManager
from lstore.latch import Latch

#control = BoundedSemaphore(1)

global tables
global control
global icon
global lmcon
global cont
global update_latch
global diskManager
global access
global lockManager
global fakeLockManager
tables = []
control = Latch()
icon = Latch()
lmcon = Latch()
access = BoundedSemaphore(1)
cont = BoundedSemaphore(1)
update_latch = Latch()
diskManager = DiskManager()
lockManager = LockManager()
fakeLockManager = FakeLockManager()

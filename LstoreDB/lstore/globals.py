from threading import BoundedSemaphore
from lstore.disk_manager import DiskManager
from lstore.lock_manager import LockManager
#control = BoundedSemaphore(1)

global tables
global control
global cont
global diskManager
global access
global lockManager
tables = []
control = BoundedSemaphore(1)
access = BoundedSemaphore(1)
cont = BoundedSemaphore(1)
diskManager = DiskManager()
lockManager = LockManager()

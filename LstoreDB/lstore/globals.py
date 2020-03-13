from threading import BoundedSemaphore
from lstore.disk_manager import DiskManager
from lstore.latch import Latch
#control = BoundedSemaphore(1)

global tables
global control
global cont
global diskManager
global access
tables = []
control = Latch()
access = BoundedSemaphore(1)
cont = BoundedSemaphore(1)
diskManager = DiskManager()

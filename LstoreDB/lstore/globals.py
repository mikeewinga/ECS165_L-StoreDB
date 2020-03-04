from threading import BoundedSemaphore
from lstore.disk_manager import DiskManager
#control = BoundedSemaphore(1)

global tables
global control
global diskManager
tables = []
control = BoundedSemaphore(1)
diskManager = DiskManager()

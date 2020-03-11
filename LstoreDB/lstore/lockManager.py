
# the pseudo lockerManager class to test if transaction.py actually works

from lstore.table import *
from lstore.pagerange import *
from lstore.transaction import *
from lstore.query import *
from lstore.index import *


class lockManager:
    def __init__(self):
        self.table_name = table_name
        self.actionName = actionName
        
    def getLock(self, table_name, readResults, actionName):

        
        print ("LOCK ACQUIRED")
        return True

    def releaseLock(self, table_name, readResults, actionName):
        print ("LOCK RELEASED")
        return True

# def acquire_lock(self, rid, actionName):
#         #FIXME =)
#         global lockManager
#         return lockManager.getLock(self.table_name, self.index.read(rid), actionName)

#     def release_lock(self, rid, actionName):
#         global lockManager
#         return lockManager.releaseLock(self.table_name, self.index.read(rid), actionName)
import threading
from config import *



class LockManager:
    def __init__(self):
        self.locks = {}
        pass
    
    #def getlock(self, type_l, level, index):
    def getlock(self, tabOrAddr):
        thread_id = threading.get_ident()

        target = self.locks.get(thread_id)
        if target:
            pass #TODO stuff
        else:
            self.locks[thread_id] = {} #insert into locks
        




        #check if thread already has needed lock
        #go from top level down and acquire locks
        #check: if acquired IS on higher level, cannot get X or IX on lower level 



class Lock:
    #def __init__(self, type_l, level, index):
    def __init__(self, table, page_range, page, row):
        self.table = table
        self.page_range = page_range
        self.page = page
        self.row = row

    




from lstore.index import *
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        # self.lockedQueries = [] # store the queries with acquired locks
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still 
    # return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args, action = ACQUIRE_LOCK, status = UNFINISHED)
            # If the query has failed to take the locks the transaction should abort
            if result == False:
                return self.abort()
            # else: # store all the queries with acquired locks
            #     self.lockedQueries.append((query,args))
        return self.commit()

    def abort(self):
        #ask database/lock manager to release the locks taken so far
        # for query, args in self.lockedQueries:
        #     query(*args, action = RELEASE_LOCK, status = ABORTED)
        # self.lockedQueries.clear()
        lstore.globals.lockManager.releaseLock()
        return False

    def commit(self):
        # call the query functions to commit
        for query, args in self.queries:
            query(*args, action = COMMIT_ACTION, status = UNFINISHED)
        # release all the locks at once
        # for query, args in self.queries:
        #     query(*args, action = RELEASE_LOCK, status = COMMITTED)
        # self.lockedQueries.clear()
        lstore.globals.lockManager.releaseLock()
        return True





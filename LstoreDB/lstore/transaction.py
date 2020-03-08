from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
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

    # For this function, we need to decide if the transaction succeeds by the 
    # locking we have
    def run(self):
        for query, args in self.queries:
            result = query(*args, action = ACQUIRE_LOCK)
            # If the query has failed to take the locks the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #ask database/lock manager to release the locks taken so far
        #TODO: do roll-back and any other necessary operations
        # release the locks
        return False

    def commit(self):
        # call the query functions to commit
        for query, args in self.queries:
            query(*args, action = COMMIT_ACTION)
        # release all the locks at once
        for query, args in self.queries:
            query(*args, action = RELEASE_LOCK)
        return True





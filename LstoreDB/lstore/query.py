from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    Deletes key from database and index
    """
    def delete(self, primary_key, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            self.table.delete(primary_key)
        else:
            return self.table.delete_lock(primary_key)

    """
    # Insert a record with specified columns
    :param columns: variadic parameters of column values in a record
    """

    def insert(self, *columns, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            #package information into records and pass records to table
            self.table.insert(*columns)
        else:
            return self.table.insert_lock()

    """
    # Read columns from a record with specified key
    :param query_columns: list of bit values, 0 for unselected columns and 1
        for selected columns
    :param key: the key value to select records based on    
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, key, column, query_columns, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            record_set = self.table.select(key, column, query_columns)
            #print("select record:")  #FIXME print debug
            #for record in record_set: print(str(record))  #FIXME print debug
            return record_set
        else:
            return self.table.select_lock(key, column)

    """
    # Update a record with specified key and columns
    :param columns: variadic parameters of column values to update. Example:
        [None, None, 4, None] specifies to update 3rd column with new value
    """

    def update(self, key, *columns, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            # invalid input
            if len(columns) < 1:
                return
            # create bitmap for schema encoding with 1 in the position of updated column
            bit = 2 ** (len(columns)-1)
            schema_encoding = 0
            for x in columns:
                if x != None:
                    schema_encoding = schema_encoding + bit
                bit = bit // 2
            #ridr = int.from_bytes(rid[0], byteorder = "big")
            self.table.update(key, schema_encoding, *columns)
            #print("update columns: " + str(columns))  #FIXME print debug
        else:
            return self.table.update_lock(key)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            sum = 0
            column_agg =[]
            # create a list of 0's and 1 where 1 is in the position of aggregate column
            for x in range(0,self.table.num_columns):
                if(x == aggregate_column_index):
                    column_agg.append(1)
                else:
                    column_agg.append(0)
            # sum the values of the aggregate column in specified interval
            for n in range(start_range, (end_range+1)):
                mList = self.table.select(n, 0, column_agg)
                for cur in mList:
                    #curr = int.from_bytes(cur, byteorder = "big")
                    sum += cur.columns[aggregate_column_index]
            #print("sum operation: " + str(sum))  # FIXME print debug
            return sum
        else: # IF ALL LOCKS ACQUIRED, RETURN TRUE, ELSE RETURN FALSE
            for key in range (start_range, (end_range+1)):
                isLockAcquired = self.table.select_lock(key, self.table.key)
                if isLockAcquired == False:
                    return False
            return True

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
            if r is not False:
                updated_columns = [None] * self.table.num_columns
                updated_columns[column] = r[column] + 1
                u = self.update(key, *updated_columns)
                return u
        else:
            return self.table.update_lock(key)

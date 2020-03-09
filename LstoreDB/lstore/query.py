from lstore.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        # some sort of flgs 
        self.table = table

    """
    Deletes key from database and index
    """
    def delete(self, primary_key, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            self.table.delete(primary_key)
        elif (action == ACQUIRE_LOCK):
            self.table.delete_acquire_lock(primary_key)

    """
    # Insert a record with specified columns
    :param columns: variadic parameters of column values in a record
    """

    def insert(self, *columns, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            #package information into records and pass records to table
            self.table.insert(*columns)
        elif (action == ACQUIRE_LOCK):
            pass # FIXME

    """
    # Read columns from a record with specified key
    :param query_columns: list of bit values, 0 for unselected columns and 1
        for selected columns
    :return: list of selected records
    """

    def select(self, key, column, query_columns, action = COMMIT_ACTION):
        if (action == COMMIT_ACTION):
            return self.table.select(key, column, query_columns)
        elif (action == ACQUIRE_LOCK):
            return self.table.select_acquire_lock(key, column)

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
        elif (action == ACQUIRE_LOCK):
            return self.table.update_acquire_lock(key)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
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
            return sum
        elif (action == ACQUIRE_LOCK):
            pass # FIXME

from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        self.hasIndex = 0
        pass

    def delete(self, key):
        rid = self.index.locate(key)
        self.table.delete(rid[0])

    """
    # Insert a record with specified columns
    :param columns: variadic parameters of column values in a record
    """

    def insert(self, *columns):
        schema_encoding = 0  # schema is 0 since no columns are updated yet
        record = Record(self.table.current_Rid_base, self.table.key, columns)
        self.table.insert(schema_encoding, record)
        #print(record)

    """
    # Read columns from a record with specified key
    :param query_columns: list of bit values, 0 for unselected columns and 1
        for selected columns
    :return: list of selected records
    """

    def select(self, key, query_columns):
        # create index for column if needed
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
        #for key, value in self.index.indexDict.items():
        #    print(key, value)
        rid = self.index.locate(key)
        record_set = []
        for x in range(0,len(rid)):
            record_set.append(Record(rid, key,
            self.table.return_record(rid[0], query_columns)))
        #print(record_set)
        return record_set

    """
    # Update a record with specified key and columns
    :param columns: variadic parameters of column values to update. Example:
        [None, None, 4, None] specifies to update 3rd column with new value
    """

    def update(self, key, *columns):
        # create index for column if needed
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
        # create bitmap for schema encoding with 1 in the position of updated column
        schema_encoding = 1 << (self.table.num_columns - 1)
        for x in columns:
            if x == None:
                schema_encoding >>= 1
            else:
                break
        rid = self.index.locate(key)
        record = Record(0, self.table.key, columns)
        self.table.update(rid[0], schema_encoding, record)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # create index for column if needed
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
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
            RID = self.index.locate(n)
            for cur in RID:
                sum += self.table.return_record(cur, column_agg)[aggregate_column_index]
        return sum

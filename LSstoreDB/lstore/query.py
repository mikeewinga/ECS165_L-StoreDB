from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        #self.index.create_index(table.key)
        pass

    """
    Deletes key from database and index
    """
    def delete(self, key):
        rid = self.index.locate(0, key)
        rid = int.from_bytes(rid[0], byteorder = "big")
        self.index.delete(key, self.table.key)
        self.table.delete(rid[0])
        

    """
    # Insert a record with specified columns
    :param columns: variadic parameters of column values in a record
    """

    def insert(self, *columns):
        schema_encoding = 0  # schema is 0 since no columns are updated yet
        record = Record(self.table.current_Rid_base, self.table.key, columns)
        self.table.insert(schema_encoding, record)

    """
    # Read columns from a record with specified key
    :param query_columns: list of bit values, 0 for unselected columns and 1
        for selected columns
    :return: list of selected records
    """

    def select(self, key, column, query_columns):
        # create index for column if needed
        self.index.create_index(column)
        rid = self.index.locate(0, key)
        record_set = []
        for item in rid:
            itemr = int.from_bytes(item, byteorder = "big")
            record_set.append(Record(itemr, key,
            self.table.return_record(itemr, query_columns)))
        return record_set

    """
    # Update a record with specified key and columns
    :param columns: variadic parameters of column values to update. Example:
        [None, None, 4, None] specifies to update 3rd column with new value
    """

    def update(self, key, *columns):
        # create index for column if needed
        self.index.create_index(self.table.key)
        if len(columns) < 1:
            return
        # create bitmap for schema encoding with 1 in the position of updated column
        bit = 2 ** (len(columns)-1)
        schema_encoding = 0
        for x in columns:
            if x != None:
                schema_encoding = schema_encoding + bit
            bit = bit // 2
        rid = self.index.locate(self.table.key, key)
        ridr = int.from_bytes(rid[0], byteorder = "big")
        self.index.update(rid[0], self.table.return_record(ridr, [1, 1, 1, 1, 1]), *columns)
        record = Record(0, self.table.key, columns)
        for item in rid:
            itemr = int.from_bytes(item, byteorder = "big")
            self.table.update(itemr, schema_encoding, record)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # create index for column if needed
        self.index.create_index(self.table.key)
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
            RID = self.index.locate(0, n)
            for cur in RID:
                curr = int.from_bytes(cur, byteorder = "big")
                sum += self.table.return_record(curr, column_agg)[aggregate_column_index]
        return sum

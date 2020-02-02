from table import Table, Record
from index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        self.hasIndex = 0
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        rid = self.index.locate(key)
        self.table.delete(rid[0])

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        schema_encoding = 0
        record = Record(self.table.current_Rid, self.table.key, columns)
        self.table.insert(schema_encoding, record)

    """
    # Read a record with specified key
    :param query_columns: list of bit values, 0 for unselected columns and 1
        for selected columns
    """

    def select(self, key, query_columns):
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
        #for key, value in self.index.indexDict.items():
        #    print(key, value)
        rid = self.index.locate(key)
        record_set = []
        for x in range(0,len(rid)):
            record_set.append(self.table.return_record(rid[0], query_columns))
        return record_set

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        schema_encoding = 1 << (self.table.num_columns - 1)
        for x in columns:
            if x == None:
                schema_encoding >>= 1
            else:
                break
        rid = self.index.locate(key)
        record = Record(rid[0], self.table.key, columns)
        self.table.update(schema_encoding, record)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        for key in range(end_range - start_range):
            rid = self.index.locate(key)
            sum += self.table.select_col_value(rid[0], aggregate_column_index)
        return sum

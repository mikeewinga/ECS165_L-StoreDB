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
        record = Record(self.table.current_Rid_base, self.table.key, columns)
        self.table.insert(schema_encoding, record)
        print(record)

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
        print(record_set)
        return record_set

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
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
        if (self.hasIndex == 0) :
            self.index.create_index(self.index.table,0)
            self.hasIndex = 1
        sum = 0
        column_agg =[]
        for x in range(0,self.table.num_columns):
            if(x == aggregate_column_index):
                column_agg.append(1)
            else:
                column_agg.append(0)
        for n in range(start_range+1, (start_range+end_range+1)):
            sum += self.table.return_record(n, column_agg)[0]
        return sum

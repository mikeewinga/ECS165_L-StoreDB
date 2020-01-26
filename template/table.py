from page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, key, num_columns):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        for x in range((self.num_columns + 4)*2):
            self.page_directory[x] = Page()
        self.current_Rid = 0
        pass

    def insert(self, schema, record):
        self.page_directory[0].write(0)
        self.page_directory[1].write(self.current_Rid)
        self.page_directory[2].write(0)
        self.page_directory[3].write(schema)
        self.current_Rid = self.current_Rid + 1
        print(record.columns)
        for x in range(self.num_columns):
            self.page_directory[x + 4].write(record.columns[x])
            
    def debugRead(self, index):
        for x in range(4 + self.num_columns):
            print(int.from_bytes(self.page_directory[x].read(index), byteorder = "big"), end =" ")
        print()

    def __merge(self):
        pass
 

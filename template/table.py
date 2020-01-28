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
        self.total_columns = num_columns + 4
        self.page_directory = {}
        for x in range((self.num_columns + 4)):
            self.page_directory[(0,x)] = Page()
            self.page_directory[(1,x)] = Page()
        self.current_Rid = 0
        pass

    def insert(self, schema, record):
        offSet = 0;
        while not self.page_directory[(0,offSet)].has_capacity():
            offSet = offSet + self.num_columns + 4
        self.page_directory[(0,0+offSet)].write(0)
        self.page_directory[(0,1+offSet)].write(self.current_Rid)
        self.page_directory[(0,2+offSet)].write(0)
        self.page_directory[(0,3+offSet)].write(schema)
        self.current_Rid = self.current_Rid + 1
        #print(self.current_Rid)
        for x in range(self.num_columns):
            self.page_directory[(0,x + 4+offSet)].write(record.columns[x])
        if not self.page_directory[(0,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(0,x + self.total_columns)] = Page()
            self.total_columns = self.total_columns + self.num_columns + 4
            
            
    def debugRead(self, index):
        offSet = (int)(index // (PAGESIZE/DATASIZE))*(4+self.num_columns)
        newIndex = (int)(index % (PAGESIZE/DATASIZE))
        for x in range(4 + self.num_columns):
            print(int.from_bytes(self.page_directory[(0,x+offSet)].read(newIndex), byteorder = "big"), end =" ")
        print()

    def __merge(self):
        pass
 

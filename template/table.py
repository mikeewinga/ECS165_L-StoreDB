from page import *
from time import time
import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        
class Directory:

    def __init__(self):
        self.indexer = {}
        

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
        stamp = datetime.datetime.now()
        data = bytearray(8)
        data[0:1] = stamp.year.to_bytes(2,byteorder = "big")
        data[2] = stamp.month
        data[3] = stamp.day
        data[4] = stamp.hour
        data[5] = stamp.minute
        data[6] = stamp.second
        #print(''.join(format(x, '02x') for x in data))
        self.page_directory[(0,2+offSet)].write(data)
        self.page_directory[(0,3+offSet)].write(schema)
        self.current_Rid = self.current_Rid + 1
        #print(self.current_Rid)
        for x in range(self.num_columns):
            self.page_directory[(0,x + 4+offSet)].write(record.columns[x])
        if not self.page_directory[(0,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(0,x + self.total_columns)] = Page()
            self.total_columns = self.total_columns + self.num_columns + 4

    def return_record(self, rid, col_wanted):
        record_wanted = []
        page_offset=(int)(rid // (PAGESIZE/DATASIZE))*(4+self.num_columns)
        rid_offset=(int)(rid % (PAGESIZE/DATASIZE))
        for x in range(0, self.num_columns):
            if(col_wanted[x]==1):                record_wanted.append(int.from_bytes(self.page_directory[(0,4+x+page_offset)].read(rid_offset), byteorder = "big"))
        return record_wanted       
            
    def debugRead(self, index):
        offSet = (int)(index // (PAGESIZE/DATASIZE))*(4+self.num_columns)
        newIndex = (int)(index % (PAGESIZE/DATASIZE))
        for x in range(4 + self.num_columns):
            print(self.page_directory[(0,x+offSet)].read(newIndex))
            print(int.from_bytes(self.page_directory[(0,x+offSet)].read(newIndex), byteorder = "big"), end =" ")
        print()

    def __merge(self):
        pass
 

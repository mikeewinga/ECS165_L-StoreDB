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
        self.current_Rid_base = 0
        self.current_Rid_tail = 2**64
        pass

    def get_timestamp(self):
        stamp = datetime.datetime.now()
        data = bytearray(8)
        data[0:1] = stamp.year.to_bytes(2,byteorder = "big")
        data[2] = stamp.month
        data[3] = stamp.day
        data[4] = stamp.hour
        data[5] = stamp.minute
        data[6] = stamp.second
        return data

    def insert(self, schema, record):
        offSet = 0;
        while not self.page_directory[(0,offSet)].has_capacity():
            offSet = offSet + self.num_columns + 4
        self.page_directory[(0,INDIRECTION_COLUMN+offSet)].write(0)
        self.page_directory[(0,RID_COLUMN+offSet)].write(self.current_Rid_base)
        data = self.get_timestamp()
        #print(''.join(format(x, '02x') for x in data))
        self.page_directory[(0,TIMESTAMP_COLUMN+offSet)].write(data)
        self.page_directory[(0,SCHEMA_ENCODING_COLUMN+offSet)].write(schema)
        self.current_Rid_base = self.current_Rid_base + 1
        #print(self.current_Rid_base)
        for x in range(self.num_columns):
            self.page_directory[(0,x + 4+offSet)].write(record.columns[x])
        if not self.page_directory[(0,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(0,x + self.total_columns)] = Page()
            self.total_columns = self.total_columns + self.num_columns + 4

    def update(self, base_rid, tail_schema, record):
        base_page_index = (int)(base_rid // (PAGESIZE/DATASIZE))*(4+self.num_columns)
        record_offset = (int)(base_rid % (PAGESIZE/DATASIZE))
        prev_update_rid = self.page_directory[(0,INDIRECTION_COLUMN+base_page_index)].read(record_offset)

        #add new tail record
        offSet = 0;
        while not self.page_directory[(1,offSet)].has_capacity(): # finds the empty offset to insert new record at
            offSet = offSet + self.num_columns + 4
        self.page_directory[(1,INDIRECTION_COLUMN+offSet)].write(prev_update_rid) # set indir to previous update rid
        self.page_directory[(1,RID_COLUMN+offSet)].write(self.current_Rid_tail) # set the rid of tail page
        data = get_timestamp()
        self.page_directory[(1,TIMESTAMP_COLUMN+offSet)].write(data) # set the timestamp
        self.page_directory[(1,SCHEMA_ENCODING_COLUMN+offSet)].write(tail_schema) # set the schema encoding
        for x in range(self.num_columns): # copy in record data
            self.page_directory[(1,x + 4+offSet)].write(record.columns[x])
        #expand the tail page if needed
        if not self.page_directory[(1,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(1,x + self.total_columns)] = Page()
            #self.total_columns = self.total_columns + self.num_columns + 4

        # set base record indirection to rid of new tail record
        self.page_directory[(0,INDIRECTION_COLUMN+base_page_index)].write(self.current_Rid_tail)
        # change schema of base record
        cur_base_schema = self.page_directory[(0,SCHEMA_ENCODING_COLUMN+base_page_index)].read(record_offset)
        new_base_schema = cur_base_schema | tail_schema
        self.page_directory[(0,SCHEMA_ENCODING_COLUMN+base_page_index)].write(new_base_schema)

        self.current_Rid_tail = self.current_Rid_tail - 1

    def debugRead(self, index):
        offSet = (int)(index // (PAGESIZE/DATASIZE))*(4+self.num_columns) # offset is page index
        newIndex = (int)(index % (PAGESIZE/DATASIZE)) # newIndex is record index
        for x in range(4 + self.num_columns):
            print(self.page_directory[(0,x+offSet)].read(newIndex))
            print(int.from_bytes(self.page_directory[(0,x+offSet)].read(newIndex), byteorder = "big"), end =" ")
        print()

    def __merge(self):
        pass

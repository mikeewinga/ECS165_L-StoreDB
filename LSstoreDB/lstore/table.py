from lstore.page import *
from time import time
from lstore.index import Index
from lstore.config import *
from lstore.pagerange import *
import datetime


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        output = "["
        for data in self.columns:
            output += str(data) + ", "
        output += "]"
        output = output.replace(", ]", "]")
        return output


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
        self.total_base_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.total_tail_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.current_Rid_base = 1
        self.current_Rid_tail = 2**64 - 1
        self.current_Prid = 0
        self.pageranges = {}
        self.pageranges[0] = PageRange(0, self.current_Rid_base, num_columns)
        self.index = PageDirectory()
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

    """
    # Handle creation of page ranges and partition record into page ranges
    # update rid -> page range id index
    """
    def insert(self, record):
        #handles page range indexing and allocating page ranges
        prid = (self.current_Rid_base-1)//RANGESIZE
        # IF page range id is higher than current max prid -> make new page range
        if prid > self.current_Prid:
            self.current_Prid = prid
            self.pageranges[prid] = PageRange(prid, self.current_Rid_base, self.num_columns)
        #insert record into the pagerange with rid and current time
        self.pageranges[prid].insert(record, self.current_Rid_base, self.get_timestamp())
        # update rid->page range id index
        self.index.write(self.current_Rid_base, prid)
        self.current_Rid_base = self.current_Rid_base + 1


    """
    Converts the schema bit string to schema bit array
    :param schema: integer bit string
    :return: schema bit array
    """
    def getOffset(self, schema, col_num):
        if (col_num < 1):
            return []
        offset = [0] * col_num
        bit = 2 ** (col_num-1)
        itr = 0
        while (bit > 0):
            if((schema - bit) >= 0):
                offset[itr] = 1
                schema = schema - bit
            itr = itr + 1
            bit = bit // 2
        return offset

    def return_record(self, rid, col_wanted):
        record_wanted = []
        prid = self.index.read(rid)
        return self.pageranges[prid].return_record(rid, col_wanted)

    def update(self, base_rid, tail_schema, record):
        prid = self.index.read(base_rid)
        self.pageranges[prid].update(base_rid, tail_schema, record, self.current_Rid_tail, self.get_timestamp())
        self.current_Rid_tail = self.current_Rid_tail - 1
        
    def delete(self, base_rid):
        prid = self.index.read(base_rid)
        self.index.delete(base_rid)
        self.pageranges[prid].delete(base_rid)

    def debugRead(self, index):
        offSet = (int)(index // (PAGESIZE/DATASIZE))*(4+self.num_columns) # offset is page index
        newIndex = (int)(index % (PAGESIZE/DATASIZE)) # newIndex is record index
        for x in range(4 + self.num_columns):
            print(self.page_directory[(0,x+offSet)].read(newIndex))
            print(int.from_bytes(self.page_directory[(0,x+offSet)].read(newIndex), byteorder = "big"), end =" ")
        print()

    def debugReadTail(self, index):
        offSet = (int)(index // (PAGESIZE/DATASIZE))*(4+self.num_columns) # offset is page index
        newIndex = (int)(index % (PAGESIZE/DATASIZE)) # newIndex is record index
        for x in range(4 + self.num_columns):
            print(self.page_directory[(1,x+offSet)].read(newIndex))
            print(int.from_bytes(self.page_directory[(1,x+offSet)].read(newIndex), byteorder = "big"), end =" ")
        print()

    def __merge(self):
        pass

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
        self.index = Index()
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
        prid = self.current_Rid_base//RANGESIZE
        if prid > self.current_Prid:
            self.current_Prid = prid
            self.pageranges[prid] = PageRange(prid, self.current_Rid_base, self.num_columns)
        self.pageranges[prid].insert(schema, record, self.current_Rid_base, self.get_timestamp())
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
        page_Index = self.index.read(base_rid)
        base_page_index = page_Index[0][1]
        record_offset = page_Index[1]
        prev_update_rid = self.page_directory[(0,INDIRECTION_COLUMN+base_page_index)].read(record_offset)
        #print(prev_update_rid)
        # add new tail record
        # find the empty offset to insert new record at
        offSet = 0;
        while not self.page_directory[(1,offSet)].has_capacity():
            offSet = offSet + self.num_columns + NUM_METADATA_COLUMNS
        # set indirection column of tail record to previous update RID
        self.page_directory[(1,INDIRECTION_COLUMN+offSet)].write(prev_update_rid)
        # update the index page directory with tail record
        self.index.write(self.current_Rid_tail, [(1,offSet),
            self.page_directory[(1,RID_COLUMN+offSet)].num_records])
        # set the RID of tail record
        self.page_directory[(1,RID_COLUMN+offSet)].write(self.current_Rid_tail)
        # set the timestamp and schema encoding
        data = self.get_timestamp()
        self.page_directory[(1,TIMESTAMP_COLUMN+offSet)].write(data)
        self.page_directory[(1,SCHEMA_ENCODING_COLUMN+offSet)].write(tail_schema)
        self.page_directory[(1,BASE_RID_COLUMN+offSet)].write(base_rid)
        # copy in record data
        for x in range(self.num_columns):
            self.page_directory[(1,x + NUM_METADATA_COLUMNS+offSet)].write(record.columns[x])
        #expand the tail page if needed
        if not self.page_directory[(1,offSet)].has_capacity():
            for x in range(self.num_columns + NUM_METADATA_COLUMNS):
                self.page_directory[(1,x + self.total_tail_phys_pages)] = Page()
            self.total_tail_phys_pages = self.total_tail_phys_pages + self.num_columns + NUM_METADATA_COLUMNS

        # set base record indirection to rid of new tail record
        self.page_directory[(0,INDIRECTION_COLUMN+base_page_index)].overwrite_record(record_offset, self.current_Rid_tail)
        # change schema of base record
        cur_base_schema = self.page_directory[(0,SCHEMA_ENCODING_COLUMN+base_page_index)].read(record_offset)
        cur_base_schema = int.from_bytes(cur_base_schema,byteorder='big',signed=False)
        new_base_schema = cur_base_schema | tail_schema
        self.page_directory[(0,SCHEMA_ENCODING_COLUMN+base_page_index)].overwrite_record(record_offset, new_base_schema)

        self.current_Rid_tail = self.current_Rid_tail - 1

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

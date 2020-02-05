from lstore.page import *
from time import time
from lstore.index import Index
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
        self.total_base_phys_pages = num_columns + 4
        self.total_tail_phys_pages = num_columns + 4
        self.page_directory = {}
        for x in range((self.num_columns + 4)):
            self.page_directory[(0,x)] = Page()
            self.page_directory[(1,x)] = Page()
        self.current_Rid_base = 1
        self.current_Rid_tail = 2**64 - 1
        self.index = Index(self)
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
        self.index.write(self.current_Rid_base, [(0,offSet),
            self.page_directory[(0,RID_COLUMN+offSet)].num_records])
        self.page_directory[(0,INDIRECTION_COLUMN+offSet)].write(0)
        self.page_directory[(0,RID_COLUMN+offSet)].write(self.current_Rid_base)
        data = self.get_timestamp()
        #print(''.join(format(x, '02x') for x in data))
        self.page_directory[(0,TIMESTAMP_COLUMN+offSet)].write(data)
        self.page_directory[(0,SCHEMA_ENCODING_COLUMN+offSet)].write(schema)
        #print(self.current_Rid_base)
        for x in range(self.num_columns):
            self.page_directory[(0,x + 4+offSet)].write(record.columns[x])
        if not self.page_directory[(0,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(0,x + self.total_base_phys_pages)] = Page()
            self.total_base_phys_pages = self.total_base_phys_pages + self.num_columns + 4
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
        page_Index = self.index.read(rid)
        page_offset = page_Index[0]
        update_F = [1] * len(col_wanted)
        # saves indirection column of base page in next
        next = self.page_directory[page_Index[0]].read(page_Index[1])
        next = int.from_bytes(next, byteorder = "big")
        # goes through each column and if user wants to read the column,
        #    appends user-requested data
        for x in range(0, self.num_columns):
            if(col_wanted[x]==1):
                record_wanted.append(int.from_bytes(self.page_directory[(page_offset[0], page_offset[1]+x+4)].read(page_Index[1]), byteorder = "big"))
            else:
                record_wanted.append(None)
        # follow indirection column to updated tail records
        while next: # if next != 0, must follow tail records
            # get page number and offset of tail record
            page_Index = self.index.read(next)
            page_offset = page_Index[0]
            # get schema column of tail record
            schema = self.page_directory[(page_offset[0], page_offset[1]+SCHEMA_ENCODING_COLUMN)].read(page_Index[1])
            schema = int.from_bytes(schema, byteorder = "big")
            schema = self.getOffset(schema, len(col_wanted))
            for x in range(0, len(schema)):
                if (schema[x] == 1) and (col_wanted[x] == 1):
                    if (update_F[x] == 1):
                        update_F[x] = 0
                        # read the updated column and overwrite corresponding value in record_wanted
                        record_wanted[x] = int.from_bytes(self.page_directory[(page_offset[0], page_offset[1]+4+x)].read(page_Index[1]), byteorder = "big")
            # get next RID from indirection column
            next = self.page_directory[page_Index[0]].read(page_Index[1])
            next = int.from_bytes(next, byteorder = "big")
        return record_wanted

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
            offSet = offSet + self.num_columns + 4
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
        # copy in record data
        for x in range(self.num_columns):
            self.page_directory[(1,x + 4+offSet)].write(record.columns[x])
        #expand the tail page if needed
        if not self.page_directory[(1,offSet)].has_capacity():
            for x in range(self.num_columns + 4):
                self.page_directory[(1,x + self.total_tail_phys_pages)] = Page()
            self.total_tail_phys_pages = self.total_tail_phys_pages + self.num_columns + 4

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

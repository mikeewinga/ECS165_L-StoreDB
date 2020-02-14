from lstore.config import *
from lstore.page import *
from lstore.index import *

"""
Table class needs an b tree to show which page range it should
operate on
Index class needs to be updated to store page range index along
with the currently saved index
    Amber suggested a new class to make index more readable
"""
class PageRange:

    def __init__(self, prid, start, num_columns):
        self.prid = prid
        self.tps = 2**64
        self.base = start
        self.cap = start + RANGESIZE
        self.num_columns = num_columns
        self.total_base_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.total_tail_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.offSet = 0;
        self.index = Index()
        self.pages = {}
        for x in range((self.num_columns + NUM_METADATA_COLUMNS)):
            self.pages[(0,x)] = Page()
            self.pages[(1,x)] = Page()

    #pass in rid from table
    def insert(self, schema, record, rid, time):
        address = Address(self.prid, 0, self.offSet, self.pages[(0,self.offSet)].num_records)
        self.index.write(rid, address)
        #indirection initialized to 0
        self.pages[address+INDIRECTION_COLUMN].write(0)
        #rid taken in from table
        self.pages[address+RID_COLUMN].write(rid)
        #get and write time stamp
        self.pages[address+TIMESTAMP_COLUMN].write(time)
        #write schema passed in from table
        self.pages[address+SCHEMA_ENCODING_COLUMN].write(schema)
        #write base rid 0 for base pages
        self.pages[address+BASE_RID_COLUMN].write(0)
        for x in range(self.num_columns):
            self.pages[address+(x+NUM_METADATA_COLUMNS)].write(record.columns[x])
        if not self.pages[address.page].has_capacity():
            self.offSet = self.offSet + self.num_columns + NUM_METADATA_COLUMNS
            for x in range(self.num_columns + NUM_METADATA_COLUMNS):
                self.pages[(0,x + self.offSet)] = Page()


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

    def return_record(self, address, col_wanted):
        record_wanted = []
        update_F = [1] * len(col_wanted)
        # saves indirection column of base page in next
        next = self.page_directory[address.page].read(address.row)
        next = int.from_bytes(next, byteorder = "big")
        # goes through each column and if user wants to read the column,
        #    appends user-requested data
        for x in range(0, self.num_columns):
            if(col_wanted[x]==1):
                record_wanted.append(int.from_bytes(self.page_directory[(page_offset[0], page_offset[1]+x+4)].read(address.row), byteorder = "big"))
            else:
                record_wanted.append(None)
        # follow indirection column to updated tail records
        while next: # if next != 0, must follow tail records
            # get page number and offset of tail record
            page_Index = self.index.read(next)
            page_offset = page_Index[0]
            # get schema column of tail record
            schema = self.page_directory[(page_offset[0], page_offset[1]+SCHEMA_ENCODING_COLUMN)].read(address.row)
            schema = int.from_bytes(schema, byteorder = "big")
            schema = self.getOffset(schema, len(col_wanted))
            for x in range(0, len(schema)):
                if (schema[x] == 1) and (col_wanted[x] == 1):
                    if (update_F[x] == 1):
                        update_F[x] = 0
                        # read the updated column and overwrite corresponding value in record_wanted
                        record_wanted[x] = int.from_bytes(self.page_directory[(page_offset[0], page_offset[1]+4+x)].read(address.row), byteorder = "big")
            # get next RID from indirection column
            next = self.page_directory[page_Index[0]].read(address.row)
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

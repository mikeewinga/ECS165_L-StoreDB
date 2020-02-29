from lstore.config import *
from lstore.page import *
from lstore.index import *

"""
Table class needs an b tree to show which page range it should
operate on
Index class needs to be updated to store page range index along
with the currently saved index
"""
class PageRange:

    def __init__(self, table_name, prid, num_columns, diskManager, is_new_range, bOffset = 0, tOffset = 0):
        self.table_name = table_name
        self.prid = prid
        self.tps = 2 ** 64 - 1
        self.cur_tid = self.tps
        self.num_columns = num_columns
        self.total_base_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.total_tail_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.bOffSet = bOffset
        self.tOffSet = tOffset
        self.merge_f = 0
        self.index = PageDirectory()
        self.diskManager = diskManager
        self.base = prid * RANGESIZE
        self.cap = self.base + RANGESIZE
        self.delete_queue = []

        if (is_new_range):
            # initialize first set of base and tail pages
            for x in range((self.num_columns + NUM_METADATA_COLUMNS)):
                # self.pages[(0,x)] = Page()
                # self.pages[(1,x)] = Page()
                base_address = Address(self.prid, 0, x)
                self.diskManager.new_page(self.table_name, base_address, x)
                tail_address = Address(self.prid, 1, x)
                self.diskManager.new_page(self.table_name, tail_address, x)

    def add_pagedir_entry(self, rid, address):
        self.index.write(rid, address)

    def set_metadata(self, bOffset, tOffset):
        self.bOffSet = bOffset
        self.tOffSet = tOffset

    #pass in rid from table
    def insert(self, record, rid, time):
        address = Address(self.prid, 0, self.bOffSet)
        num_records = self.diskManager.page_num_records(self.table_name, address)
        address.row = num_records
        self.index.write(rid, address)
        #indirection initialized to 0
        self.diskManager.append_write(self.table_name, address+INDIRECTION_COLUMN, 0)
        #rid taken in from table
        self.diskManager.append_write(self.table_name, address+RID_COLUMN, rid)
        #get and write time stamp
        self.diskManager.append_write(self.table_name, address+TIMESTAMP_COLUMN, time)
        #write schema passed in from table
        self.diskManager.append_write(self.table_name, address+SCHEMA_ENCODING_COLUMN, 0)
        #write base rid 0 for base pages
        self.diskManager.append_write(self.table_name, address+BASE_RID_COLUMN, 0)
        for x in range(self.num_columns):
            self.diskManager.append_write(self.table_name, address + (x+NUM_METADATA_COLUMNS), record.columns[x])
        # expand new base pages if needed
        if not self.diskManager.page_has_capacity(self.table_name, address):
            if rid == self.cap:
                self.merge_f = 1
            self.bOffSet = self.bOffSet + self.num_columns + NUM_METADATA_COLUMNS
            for x in range(self.num_columns + NUM_METADATA_COLUMNS):
                base_address = Address(self.prid, 0, x + self.bOffSet)
                self.diskManager.new_page(self.table_name, base_address, x)


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
        address = self.index.read(rid)
        update_F = [1] * len(col_wanted)
        # saves indirection column of base page in next
        next = self.diskManager.read(self.table_name, address)
        next = int.from_bytes(next, byteorder = "big")
        # goes through each column and if user wants to read the column,
        #    appends user-requested data
        for x in range(0, self.num_columns):
            if(col_wanted[x]==1):
                user_data = self.diskManager.read(self.table_name, address+(x+NUM_METADATA_COLUMNS))
                record_wanted.append(int.from_bytes(user_data, byteorder = "big"))
            else:
                record_wanted.append(None)

        # follow indirection column to updated tail records
        while next: # if next != 0, must follow tail records
            # get page number and offset of tail record
            address = self.index.read(next)
            # get schema column of tail record
            schema = self.diskManager.read(self.table_name, address+SCHEMA_ENCODING_COLUMN)
            schema = int.from_bytes(schema, byteorder = "big")
            schema = self.getOffset(schema, len(col_wanted))
            for x in range(0, len(schema)):
                if (schema[x] == 1) and (col_wanted[x] == 1):
                    if (update_F[x] == 1):
                        update_F[x] = 0
                        # read the updated column and overwrite corresponding value in record_wanted
                        data = self.diskManager.read(self.table_name, address+(NUM_METADATA_COLUMNS+x))
                        record_wanted[x] = int.from_bytes(data, byteorder="big")
            # get next RID from indirection column
            next = self.diskManager.read(self.table_name, address)
            next = int.from_bytes(next, byteorder = "big")

        return record_wanted

    def update(self, base_rid, tail_schema, record, tid, time):
        bAddress = self.index.read(base_rid)
        address = Address(self.prid, 1, self.tOffSet)
        tail_num_rows = self.diskManager.page_num_records(self.table_name, address)
        address.row = tail_num_rows
        self.index.write(tid, address)
        self.cur_tid = tid
        prev_update_rid = self.diskManager.read(self.table_name, bAddress + INDIRECTION_COLUMN)
        # add new tail record
        # set indirection column of tail record to previous update RID
        self.diskManager.append_write(self.table_name, address+INDIRECTION_COLUMN, prev_update_rid)
        # set the RID of tail record
        self.diskManager.append_write(self.table_name, address+RID_COLUMN, tid)
        # set the timestamp and schema encoding
        self.diskManager.append_write(self.table_name, address+TIMESTAMP_COLUMN, time)
        self.diskManager.append_write(self.table_name, address+SCHEMA_ENCODING_COLUMN, tail_schema)
        #write base rid
        self.diskManager.append_write(self.table_name, address+BASE_RID_COLUMN, base_rid)
        # copy in record data
        for x in range(self.num_columns):
            self.diskManager.append_write(self.table_name, address+(x+NUM_METADATA_COLUMNS), record.columns[x])
        #expand the tail page if needed
        if not self.diskManager.page_has_capacity(self.table_name, address):
            self.tOffSet = self.tOffSet + self.num_columns + NUM_METADATA_COLUMNS
            for x in range(self.num_columns + NUM_METADATA_COLUMNS):
                tail_address = Address(self.prid, 1, x + self.tOffSet)
                self.diskManager.new_page(self.table_name, tail_address, x)

        # set base record indirection to rid of new tail record
        self.diskManager.overwrite(self.table_name, bAddress+INDIRECTION_COLUMN, tid)
        # change schema of base record
        cur_base_schema = self.diskManager.read(self.table_name, bAddress+SCHEMA_ENCODING_COLUMN)
        cur_base_schema = int.from_bytes(cur_base_schema,byteorder='big',signed=False)
        new_base_schema = cur_base_schema | tail_schema
        self.diskManager.overwrite(self.table_name, bAddress+SCHEMA_ENCODING_COLUMN, new_base_schema)

    def delete(self, base_rid):
        address = self.index.read(base_rid)
        if self.index.delete(base_rid):
            self.delete_queue.append(base_rid)
        self.diskManager.overwrite(self.table_name, address+RID_COLUMN, 0)
        # saves indirection column of base page in next
        next = self.diskManager.read(self.table_name, address)
        next = int.from_bytes(next, byteorder = "big")

        # follow indirection column to updated tail records
        while next: # if next != 0, must follow tail records
            # get page number and offset of tail record
            address = self.index.read(next)
            self.index.delete(next)
            self.diskManager.overwrite(self.table_name, address+RID_COLUMN, 0)
            # get next RID from indirection column
            next = self.diskManager.read(self.table_name, address)
            next = int.from_bytes(next, byteorder = "big")

    def get_pagedir_dict(self):
        return self.index.indexDict

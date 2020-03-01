from lstore.page import Page
from time import time
from lstore.index import Index, PageDirectory
from lstore.config import *
from lstore.pagerange import PageRange
import datetime
import copy

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


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, diskManager, control, key = None, num_columns = None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.pageranges = {}
        self.control = control
        self.index = PageDirectory()
        self.diskManager = diskManager
        if (key != None and num_columns != None): # create table from scratch
            self.current_Rid_base = 1
            self.current_Rid_tail = 2 ** 64 - 2
            self.current_Prid = 0
            self.total_base_phys_pages = num_columns + NUM_METADATA_COLUMNS
            self.total_tail_phys_pages = num_columns + NUM_METADATA_COLUMNS
            # create the first empty page range
            self.pageranges[0] = PageRange(self.name, 0, self.num_columns, diskManager, True)
        else:  # table will have to be initialized manually
            self.current_Rid_base = None
            self.current_Rid_tail = None
            self.current_Prid = None
            self.total_base_phys_pages = None
            self.total_tail_phys_pages = None

    def set_table_metadata(self, primary_key, num_user_columns, current_base_rid, current_tail_rid, current_prid):
        self.key = primary_key
        self.num_columns = num_user_columns
        self.current_Rid_base = current_base_rid
        self.current_Rid_tail = current_tail_rid
        self.current_Prid = current_prid
        self.total_base_phys_pages = self.num_columns + NUM_METADATA_COLUMNS
        self.total_tail_phys_pages = self.num_columns + NUM_METADATA_COLUMNS

    """
    :param prange_metadata: (bOffset, tOffset, cur_tid, mOffset, merge_f)
    """
    def add_page_range(self, prid, prange_metadata):
        is_new_range = False
        self.pageranges[prid] = PageRange(self.name, prid, self.num_columns, self.diskManager, is_new_range, prange_metadata)

    def add_pagedir_entry(self, rid, prid):
        #delete function
        pass

    def get_page_range(self, prid):
        return self.pageranges[prid]

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
    def insert(self, *columns):
        self.control.acquire()
        record = Record(self.current_Rid_base, self.key, columns)
        #handles page range indexing and allocating page ranges
        prid = (self.current_Rid_base-1)//RANGESIZE
        # IF page range id is higher than current max prid -> make new page range
        if prid > self.current_Prid:
            self.current_Prid = prid
            self.pageranges[prid] = PageRange(self.name, prid, self.num_columns, self.diskManager, True)
        #insert record into the pagerange with rid and current time
        self.pageranges[prid].insert(record, self.current_Rid_base, self.get_timestamp())
        # update rid->page range id index
        self.current_Rid_base = self.current_Rid_base + 1
        self.control.release()

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

    def return_record(self, rid, key, col_wanted):
        record_wanted = []
        prid = (rid-1)//RANGESIZE
        return Record(rid, key, self.pageranges[prid].return_record(rid, col_wanted))

    def update(self, base_rid, tail_schema, *columns):
        self.control.acquire()
        record = Record(0, self.key, columns)
        prid = (base_rid-1)//RANGESIZE
        self.pageranges[prid].update(base_rid, tail_schema, record, self.current_Rid_tail, self.get_timestamp())
        self.current_Rid_tail = self.current_Rid_tail - 1
        self.control.release()

    def delete(self, base_rid):
        self.control.acquire()
        prid = prid = (base_rid-1)//RANGESIZE
        self.pageranges[prid].delete(base_rid)
        self.control.release()

    def close(self):
        overall_page_directory = {}
        page_range_metadata = {}
        for prid in self.pageranges:
            pagedir_dict = self.pageranges[prid].get_pagedir_dict()
            overall_page_directory.update(pagedir_dict)
            page_range_metadata[prid] = (self.pageranges[prid].bOffSet, self.pageranges[prid].tOffSet, self.pageranges[prid].cur_tid, self.pageranges[prid].mOffSet, self.pageranges[prid].merge_f)

        # flush table and page range metadata into table index file
        self.diskManager.flush_table_metadata(self.name, self.current_Rid_base, self.current_Rid_tail, self.current_Prid)
        self.diskManager.flush_pagerange_metadata(self.name, page_range_metadata)
        self.diskManager.flush_index(self.name, self.current_Rid_base, self.current_Rid_tail, self.current_Prid)

        # flush page ranges' page directories into page directory file
        self.diskManager.flush_page_directory(self.name, overall_page_directory)


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


class Directory:

    def __init__(self):
        self.indexer = {}


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, diskManager, key = None, num_columns = None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.pageranges = {}
        self.index = PageDirectory()
        self.diskManager = diskManager
        if (key != None and num_columns != None): # create table from scratch
            self.current_Rid_base = 1
            self.current_Rid_tail = 2 ** 64 - 1
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

    def add_page_range(self, prid, bOffset, tOffset, cur_tid):
        is_new_range = False
        self.pageranges[prid] = PageRange(self.name, prid, self.num_columns, self.diskManager, is_new_range, bOffset, tOffset, cur_tid)

    def add_pagedir_entry(self, rid, prid):
        self.index.write(rid, prid)

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
    def insert(self, record):
        #handles page range indexing and allocating page ranges
        prid = (self.current_Rid_base-1)//RANGESIZE
        # IF page range id is higher than current max prid -> make new page range
        if prid > self.current_Prid:
            self.current_Prid = prid
            self.pageranges[prid] = PageRange(self.name, prid, self.num_columns, self.diskManager, True)
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

    def close(self):
        overall_page_directory = {}
        page_range_metadata = {}
        for prid in self.pageranges:
            pagedir_dict = self.pageranges[prid].get_pagedir_dict()
            overall_page_directory.update(pagedir_dict)
            page_range_metadata[prid] = (self.pageranges[prid].bOffSet, self.pageranges[prid].tOffSet, self.pageranges[prid].cur_tid)

        # flush table and page range metadata into table index file
        self.diskManager.flush_table_metadata(self.name, self.current_Rid_base, self.current_Rid_tail, self.current_Prid)
        self.diskManager.flush_pagerange_metadata(self.name, page_range_metadata)
        self.diskManager.flush_index(self.name, self.current_Rid_base, self.current_Rid_tail, self.current_Prid)

        # flush page ranges' page directories into page directory file
        self.diskManager.flush_page_directory(self.name, overall_page_directory)

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

    """
    #number of max possible physical pages to copy into merge() from a page_range's base page
    # i.e. bRID, base_schema and user data columns
    numPages =  2 + page_Range.num_columns

    #copy pages
    mergePages = {}
    for i in range (0,2): # go through both potential base pages
        #mergePages[(i,0)] = copy.deepcopy( page_Range.pages[0, SCHEMA_ENCODING_COLUMN + x*page_Range.total_base_phys_pages] )
        #mergePages[(i,1)] = copy.deepcopy( page_Range.pages[0, BASE_RID_COLUMN + x*page_Range.total_base_phys_pages] )
        
        #account for case when pageRange has only one base page
        if page_Range.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] is None:
            break
        #copy schema and bRID columns, overwrite newTPS in each column
        mergePages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] = copy.deepcopy( page_Range.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] )
        mergePages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)
  
        mergePages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)] = copy.deepcopy( page_Range.pages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)] )
        mergePages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)
        #copy user data columns and overwrite newTPS
        for  j in range (page_Range.num_columns):
            mergePages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)] = copy.deepcopy( page_Range.pages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)] )
            mergePages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)
            #print("index: ", NUM_METADATA_COLUMNS +  j + x*page_Range.total_base_phys_pages)
        
    """

    def merge(self, page_Range):
        #we get page_range passed to us,containing
        #   - startRID and endRID in page range
        #   - query_queue containing active queries (with timestamps of when they started)
        #   - delete_queue containing delete requests for the page_range
        #   - TPS (rid of last merged tail record)
        #   - TID for latest updated tail record ->
        # so we iterate from (latest upated) TID to TPS
        # and combine updates until we get most up-to-date data for all records

        #get time of when merge() started
        #mergeStartTime = datetime.datetime.now()

        #clear delete_queue
        page_Range.delete_queue = []
    
        #create copy of base pages (bRID, base schema, user data pages)and insert newTPS in each of them
        mergeRange = copy.deepcopy(page_Range)
        tid = mergeRange.cur_tid

        address = mergeRange.index.read(tid)
        t_page = address.pagenumber
        t_row = address.row
        step = NUM_METADATA_COLUMNS + mergeRange.num_columns

        #look at last tail page, potentially not full
        for cur_page in range(t_page, -1, -step):
            for recNum in range (t_row, 0, -1):
                base_rid = mergeRange.pages[(1, cur_page+BASE_RID_COLUMN)].read(recNum)
                base_rid = int.from_bytes(base_rid, byteorder = "big")

                baddress = mergeRange.index.read(base_rid)

                tSchema = mergeRange.pages[(1, cur_page+SCHEMA_ENCODING_COLUMN)].read(recNum)
                bSchema = mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].read(baddress.row)
                tSchema = int.from_bytes(tSchema, byteorder = "big")
                bSchema = int.from_bytes(bSchema, byteorder = "big")

                schemaToUpdate = bSchema & tSchema #bitwise AND

                resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT

                # split schemaToUpdate into bool array [0,1,0,...]
                schemaToUpdate = self.getOffset(schemaToUpdate, mergeRange.num_columns)

                for x in range(0, len(schemaToUpdate)):
                    if (schemaToUpdate[x]):
                        value = int.from_bytes(mergeRange.pages[(1, cur_page+NUM_METADATA_COLUMNS+x)].read(recNum), byteorder = "big")
                        mergeRange.pages[baddress+(NUM_METADATA_COLUMNS+x)].overwrite_record(baddress.row, value)

                #convert new base schema to binary and store back to base record
                mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].overwrite_record(baddress.row, resultingBaseSchema)
                
            t_row = 511

        #apply delete requests from delete_queue
        #for deletion in page_Range.delete_queue:
            #mergeRange.delete_queue.delete_queue(d)
        page_Range.pages = mergeRange.pages

        #replace
        
        #page_Range.tps = newTPS


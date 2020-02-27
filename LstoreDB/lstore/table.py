from lstore.page import *
from time import time
from lstore.index import Index
from lstore.config import *
from lstore.pagerange import *
import datetime
import copy

class Record:
    
    """
    :param rid: int
    :param key : int        # Number of column to hold key values
    :param columns : array  # To hold user data
    """
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

        
    """
    Prints record
    """
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
    def __init__(self, name, key, num_columns, diskManager):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_base_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.total_tail_phys_pages = num_columns + NUM_METADATA_COLUMNS
        self.current_Rid_base = 1
        self.current_Rid_tail = 2**64 - 1
        self.current_Prid = 0
        self.pageranges = {}
        self.pageranges[0] = PageRange(self.name, 0, self.current_Rid_base, num_columns, diskManager)
        self.index = PageDirectory()
        self.diskManager = diskManager
        pass

    """
    Returns timestamp(current time) in correct format
    """
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
    Inserts record(param record) into table
    Handles creation of page ranges and partition record into page ranges
    # update rid -> page range id index
    """
    def insert(self, record):
        #handles page range indexing and allocating page ranges
        prid = (self.current_Rid_base-1)//RANGESIZE
        # IF page range id is higher than current max prid -> make new page range
        if prid > self.current_Prid:
            self.current_Prid = prid
            self.pageranges[prid] = PageRange(self.name, prid, self.current_Rid_base, self.num_columns, self.diskManager)
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

    """
    Returns record by rid that has data or None in its fields (according to col_wanted) 
    
    """
    def return_record(self, rid, col_wanted):
        record_wanted = []
        prid = self.index.read(rid)
        return self.pageranges[prid].return_record(rid, col_wanted)

    
    """
    Updates a record(that has rid=base_rid) using (param)record and it's fields according to tail_schema
    
    """
    def update(self, base_rid, tail_schema, record):
        prid = self.index.read(base_rid)
        self.pageranges[prid].update(base_rid, tail_schema, record, self.current_Rid_tail, self.get_timestamp())
        self.current_Rid_tail = self.current_Rid_tail - 1

    """
    Deletes a record by rid(param base_rid)
    
    """
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
        

    """
    Merges page_Range passed to it as an argument
    """
    #page_Range contains:
        #   - startRID and endRID in page range
        #   - delete_queue containing delete requests for the page_range (i.e. rids to be deleted at the end of merge)
        #   - TPS (rid of last merged tail record)
        #   - TID (rid of latest updated tail record)
        # so we iterate from (latest upated) TID to TPS
        # and combine updates until we get most up-to-date data for all records
    def merge(self, page_Range):
        
        #get time of when merge() started
        #mergeStartTime = datetime.datetime.now()

        #clear delete_queue
        page_Range.delete_queue = []
    
        #create copy of base pages (bRID, base schema, user data pages)and insert newTPS in each of them
        mergeRange = copy.deepcopy(page_Range)
        tid = mergeRange.cur_tid

        #get address of last updated tail record
        address = mergeRange.index.read(tid)
        
        #get page number containing last updated tail record
        t_page = address.pagenumber
        
        #get row number containing last updated tail record
        t_row = address.row
        
        #get total number of columns (meta columns and user data columns)
        step = NUM_METADATA_COLUMNS + mergeRange.num_columns

        #loop first through last tail page, potentially not full, 
        #then loop through all the rest full tail pages
        #AND apply update from each tail record to corresponding base record
        for cur_page in range(t_page, -1, -step):
            for recNum in range (t_row, 0, -1):
                base_rid = mergeRange.pages[(1, cur_page+BASE_RID_COLUMN)].read(recNum)
                base_rid = int.from_bytes(base_rid, byteorder = "big")

                baddress = mergeRange.index.read(base_rid)

                tSchema = mergeRange.pages[(1, cur_page+SCHEMA_ENCODING_COLUMN)].read(recNum)
                bSchema = mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].read(baddress.row)
                tSchema = int.from_bytes(tSchema, byteorder = "big")
                bSchema = int.from_bytes(bSchema, byteorder = "big")

                #bit string holding ones for fields to update and zeroes for fields that don't need to be updated
                schemaToUpdate = bSchema & tSchema #bitwise AND

                #bit string holding resulting schema after update is applied
                resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT

                # split schemaToUpdate into bool array [0,1,0,...]
                schemaToUpdate = self.getOffset(schemaToUpdate, mergeRange.num_columns)

                #loop through schemaToUpdate and apply updates to base record fields with corresponding ones in schemaToUpdate
                for x in range(0, len(schemaToUpdate)):
                    if (schemaToUpdate[x]):
                        value = int.from_bytes(mergeRange.pages[(1, cur_page+NUM_METADATA_COLUMNS+x)].read(recNum), byteorder = "big")
                        mergeRange.pages[baddress+(NUM_METADATA_COLUMNS+x)].overwrite_record(baddress.row, value)

                #convert resultingBaseSchema to binary and store back to base record
                mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].overwrite_record(baddress.row, resultingBaseSchema)
                
            #update t_row index for iterating in the loop through full tail pages
            t_row = 511

        #apply delete requests from delete_queue
        #for deletion in page_Range.delete_queue:
            #mergeRange.delete_queue.delete_queue(d)
            
        #replace original base pages with merged pages
        page_Range.pages = mergeRange.pages

        #page_Range.tps = newTPS


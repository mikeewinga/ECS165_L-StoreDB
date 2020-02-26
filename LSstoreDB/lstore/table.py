from lstore.page import *
from time import time
from lstore.index import Index
from lstore.config import *
from lstore.pagerange import *
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


    def insert(self, record):
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

    def merge(self, page_Range):
       
        mergeStartTime = datetime.datetime.now()

        page_Range.delete_queue.clear()
    
        tid = page_Range.cur_tid

        newTPS = page_Range.cur_tid
    
        mergeRange = copy.deepcopy(page_Range)

        start_row = mergeRange.index.read(tid).row

        address = mergeRange.index.read(tid)

        numPages = address.pagenumber

        factor = 0; # increment this each time by 1
        temp = address + (RID_COLUMN + factor * (-10)); # change this if you want

        while True:

            for recNum in range (start_row, 0, -1):
                
                nextTid = mergeRange.pages[temp].read(recNum)
                
                nextTid = int.from_bytes(nextTid, byteorder = "big")

                address = mergeRange.index.read(nextTid)

                baddress = mergeRange.index.read(recNum)

                print("address #", recNum, ": ", address)
                tSchema = mergeRange.pages[address+SCHEMA_ENCODING_COLUMN].read(address.row)
                bSchema = mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].read(baddress.row)

                tSchema = int.from_bytes(tSchema, byteorder = "big")
                bSchema = int.from_bytes(bSchema, byteorder = "big")
            
                schemaToUpdate = bSchema & tSchema #bitwise AND
                resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT

                strSchemaToUpdate = "{0:{fill}5b}".format(schemaToUpdate, fill='0')
               
                for x in range(0, len(strSchemaToUpdate)):
                    if (strSchemaToUpdate[x] == '1'):
                        value = int.from_bytes(page_Range.pages[address+(NUM_METADATA_COLUMNS+x)].read(address.row), byteorder = "big")
                        mergeRange.pages[baddress+(NUM_METADATA_COLUMNS+x)].overwrite_record(baddress.row, value)

                resultingBaseSchema = resultingBaseSchema.to_bytes(8, byteorder = "big")
                mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].overwrite_record(baddress.row, resultingBaseSchema)

            start_row = 511
            factor += 1
            temp = address + (RID_COLUMN + factor * (-10))
            print(temp)
            if (temp[1]) < 0:
                break

        for d in page_Range.delete_queue:
            mergeRange.delete_queue.delete_queue(d)

        
        page_Range.tps = newTPS
        
        for i in range (0,2): # go through both potential base pages
            
            if page_Range.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] is None:
                break

            page_Range.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] = copy.deepcopy( mergeRange.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)] )
            page_Range.pages[(0, SCHEMA_ENCODING_COLUMN + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)
      
            page_Range.pages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)] = copy.deepcopy( mergeRange.pages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)] )
            page_Range.pages[(0, BASE_RID_COLUMN + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)

            for  j in range (page_Range.num_columns):
                page_Range.pages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)] = copy.deepcopy( mergeRange.pages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)] )
                page_Range.pages[(0, NUM_METADATA_COLUMNS +  j + i*page_Range.total_base_phys_pages)].overwrite_record(0, newTPS)
                        
        numPages = int(page_Range.tOffSet / page_Range.total_tail_phys_pages)
        print("\n\n\nPagerange Offset: ", page_Range.tOffSet, "\n\n\n")
         
        print("end of merge")
        pass


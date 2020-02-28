from lstore.table import Table
from lstore.disk_manager import DiskManager
from lstore.config import *
from lstore.index import Address
import threading
from threading import BoundedSemaphore
import time
import copy

def getOffset(schema, col_num):
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

def merge(table, page_Range):
    global control
    global diskManager
    control.acquire()
    #acquire all required resources that are time critical
    #clear delete_queue
    page_Range.delete_queue = []
    #create copy of base pages (bRID, base schema, user data pages)and insert newTPS in each of them
    b_pages = {}
    p_ind = 0
    step = NUM_METADATA_COLUMNS + page_Range.num_columns
    tid = page_Range.cur_tid
    mindex = copy.deepcopy(page_Range.index)
    
    while p_ind < page_Range.bOffSet:
        for i in range(0, page_Range.num_columns+NUM_METADATA_COLUMNS):
            b_pages[i+p_ind] = diskManager.merge_copy_page(table, Address(page_Range.prid, 0, p_ind), i)
        p_ind = p_ind + step
        
    #page_Range.merge_helper()

    control.release()
    needs = [1,3]
    for i in range(0,page_Range.num_columns):
        needs.append(NUM_METADATA_COLUMNS+i)
    address = mindex.read(tid)
    t_page = address.pagenumber
    t_row = address.row

    #diskManager.debug_print_page(table, b_pages[(0, 1)])
    #look at last tail page, potentially not full
    for cur_page in range(t_page, -1, -step):
        for recNum in range (t_row, 0, -1):
            address = Address(page_Range.prid, 1, cur_page, recNum)
            base_rid = diskManager.read(table, address+4)
            base_rid = int.from_bytes(base_rid, byteorder = "big")
            baddress = mindex.read(base_rid).copy()
            b = baddress
            tSchema = diskManager.read(table, address+SCHEMA_ENCODING_COLUMN)
            bSchema = diskManager.read(table, baddress+SCHEMA_ENCODING_COLUMN)
            tSchema = int.from_bytes(tSchema, byteorder = "big")
            bSchema = int.from_bytes(bSchema, byteorder = "big")

            schemaToUpdate = bSchema & tSchema #bitwise AND

            resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT
            taddress = baddress
            taddress.change_flag(2)
            # split schemaToUpdate into bool array [0,1,0,...]
            schemaToUpdate = getOffset(schemaToUpdate, page_Range.num_columns)
            #if cur_page == 0 and recNum == 1:
            for x in range(0, len(schemaToUpdate)):
                if (schemaToUpdate[x]):
                    value = diskManager.read(table, address+(NUM_METADATA_COLUMNS+x))
                    value = int.from_bytes(value, byteorder = "big")
                    target = taddress+(NUM_METADATA_COLUMNS+x)
                    diskManager.overwrite(table, target, value)

            #convert new base schema to binary and store back to base record
            #mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].overwrite_record(baddress.row, resultingBaseSchema)
        t_row = 511
    p_ind = 0
    control.acquire()
    while p_ind < page_Range.bOffSet:
        for x in needs:
            address = Address(page_Range.prid, 0, p_ind+x)
            diskManager.merge_replace_page(table, address)
        p_ind = p_ind + step
    #page_Range.pages = mergeRange.pages
    #handle delete queue
    #handle swapping tal records
    control.release()

global control

def mergeLoop():
    t_ind = 0
    pr_ind = 5
    global tables
    global diskManager
    global control
    while 1:
        if t_ind < len(tables):
            pagenum = len(tables[t_ind].pageranges)
            while pr_ind < pagenum:
                if(tables[t_ind].pageranges[pr_ind].merge_f and tables[t_ind].pageranges[pr_ind].tOffSet):
                    merge(tables[t_ind].name, tables[t_ind].pageranges[pr_ind])
                pr_ind = pr_ind + 1
            pr_ind = 0
            t_ind = t_ind + 1
        else:
            time.sleep(0)
            t_ind = 0


class Database():

    def __init__(self):
        global tables
        tables = []
        global diskManager
        diskManager = DiskManager()
        self.diskManager = diskManager
        global control
        control = BoundedSemaphore(1)
        self.control = control
        merger = threading.Thread(target=mergeLoop)
        merger.start()
        pass

    def open(self, path):
        self.diskManager.set_directory_path(path)
        pass

    def close(self):
        self.diskManager.close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if (self.diskManager.new_table_file(name, key, num_columns)):  # check if new table file was successfully created
            table = Table(name, key, num_columns, self.diskManager, self.control)
            tables.append(table)
            return table
        else:
            return None
        #self.tables[name] = table
        #self.num_tables = self.num_tables + 1

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        #FIXME
        if name in self.tables:
            del self.tables[name]
            self.num_tables = self.num_tables - 1
        pass

    """
    # Retruns table with the passed name
    """
    def get_table(self, name):
        table_metadata = self.diskManager.open_table_file(name)
        if (len(table_metadata) != 0):  # the table file and metadata exist
            table = Table(name, table_metadata[PRIMARY_KEY], table_metadata[COLUMNS], self.diskManager)
            return table
        else:
            return None

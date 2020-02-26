from lstore.table import Table
from lstore.disk_manager import DiskManager
from lstore.config import *
import threading
from threading import BoundedSemaphore
import time

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

def merge(page_Range):
    global control
    control.acquire()
    #acquire all required resources that are time critical
    #give page range new tail record
    #function to change tail record index
    control.release()

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
            schemaToUpdate = getOffset(schemaToUpdate, mergeRange.num_columns)

            for x in range(0, len(schemaToUpdate)):
                if (schemaToUpdate[x]):
                    value = int.from_bytes(mergeRange.pages[(1, cur_page+NUM_METADATA_COLUMNS+x)].read(recNum), byteorder = "big")
                    mergeRange.pages[baddress+(NUM_METADATA_COLUMNS+x)].overwrite_record(baddress.row, value)

            #convert new base schema to binary and store back to base record
            mergeRange.pages[baddress+SCHEMA_ENCODING_COLUMN].overwrite_record(baddress.row, resultingBaseSchema)
            
        t_row = 511

    control.acquire()
    page_Range.pages = mergeRange.pages
    #handle delete queue
    #handle swapping tal records
    control.release()

global control

def mergeLoop():
    t_ind = 0
    pr_ind = 0
    global tables
    global diskManager
    global control
    while 1:
        control.acquire()
        print("nab")
        control.release()
        time.sleep(0)
        """
        if t_ind < len(tables):
            pagenum = len(tables[t_ind].pageranges)
            #print(pagenum)
            t_ind = t_ind + 1
        else:
            time.sleep(0)
            t_ind = 0
        """


class Database():

    def __init__(self):
        init()
        global tables
        tables = []
        global control
        control = BoundedSemaphore(1)
        self.control = control
        #self.tables = {}  # maps {string name : Table}
        #self.num_tables = 0
        global diskManager
        diskManager = DiskManager()
        self.diskManager = diskManager
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
            table = Table(name, table_metadata[PRIMARY_KEY], table_metadata[COLUMNS], self.diskManager, self.control)
            return table
        else:
            return None

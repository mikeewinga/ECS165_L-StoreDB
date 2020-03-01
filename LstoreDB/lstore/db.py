from lstore.table import Table
from lstore.disk_manager import DiskManager
from lstore.config import *
from lstore.index import Address
from threading import BoundedSemaphore
import threading
import time
import copy
import sys

global control

class Merger:
    def __init__(self):
        self._running = 1

    def getOffset(self,schema, col_num):
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

    def merge(self,table, page_Range):
        global control
        global diskManager
        control.acquire()
        print("merging", page_Range.prid, page_Range.tOffSet)
        #acquire all required resources that are time critical
        #clear delete_queue
        page_Range.delete_queue = []
        #create copy of base pages (base schema, user data pages)and insert newTPS in each of them
        b_pages = {}
        p_ind = 0
        step = NUM_METADATA_COLUMNS + page_Range.num_columns
        tid = copy.deepcopy(page_Range.cur_tid)
        stoper = page_Range.mOffSet - 1
        mindex = copy.deepcopy(page_Range.index)
        needs = [3]
        for i in range(0, page_Range.num_columns):
            needs.append(i+NUM_METADATA_COLUMNS)
        while p_ind < page_Range.bOffSet:
            for i in range(page_Range.num_columns+NUM_METADATA_COLUMNS):#needs:
                b_pages[i+p_ind] = diskManager.merge_copy_page(table, Address(page_Range.prid, 0, p_ind+i), i)
            p_ind = p_ind + step
        page_Range.merge_helper()
        page_Range.mOffSet = copy.deepcopy(page_Range.tOffSet)
        #control.release()


        address = mindex.read(tid)
        t_page = address.pagenumber
        t_row = address.row

        #diskManager.debug_print_page(table, b_pages[(0, 1)])
        #look at last tail page, potentially not full
        for cur_page in range(t_page, stoper, -step):
            for recNum in range (t_row, 0, -1):
                address = Address(page_Range.prid, 1, cur_page, recNum)
                base_rid = diskManager.read(table, address+BASE_RID_COLUMN)
                base_rid = int.from_bytes(base_rid, byteorder = "big")
                baddress = mindex.read(base_rid).copy()
                b = baddress
                baddress.change_flag(2)
                tSchema = diskManager.read(table, address+SCHEMA_ENCODING_COLUMN)
                bSchema = diskManager.read(table, baddress+SCHEMA_ENCODING_COLUMN)
                tSchema = int.from_bytes(tSchema, byteorder = "big")
                bSchema = int.from_bytes(bSchema, byteorder = "big")

                schemaToUpdate = bSchema & tSchema #bitwise AND

                resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT
                # split schemaToUpdate into bool array [0,1,0,...]
                schemaToUpdate = self.getOffset(schemaToUpdate, page_Range.num_columns)
                #if cur_page == 0 and recNum == 1:
                for x in range(0, len(schemaToUpdate)):
                    if (schemaToUpdate[x]):
                        value = diskManager.read(table, address+(NUM_METADATA_COLUMNS+x))
                        value = int.from_bytes(value, byteorder = "big")
                        target = baddress+(NUM_METADATA_COLUMNS+x)
                        diskManager.overwrite(table, target, value)
                diskManager.overwrite(table, baddress+SCHEMA_ENCODING_COLUMN, resultingBaseSchema)

            t_row = 511
        p_ind = 0
        #control.acquire()
        #handle delete queue
        for rid in page_Range.delete_queue:
            address = mindex.read(rid).copy()
            address.change_flag(2)
            diskManager.overwrite(table, address+RID_COLUMN, 0)
        #swap pages
        while p_ind < page_Range.bOffSet:
            for x in needs:
                address = Address(page_Range.prid, 0, p_ind+x)
                print(address.pagerange, address.page)
                diskManager.merge_replace_page(table, address)
            p_ind = p_ind + step
        while p_ind < page_Range.bOffSet:
            for i in range(page_Range.num_columns+NUM_METADATA_COLUMNS):
                diskManager.overwrite(table, Address(page_Range.prid, 0, i+p_ind,0), tid)
            p_ind = p_ind + step
        page_Range.tps = tid
        control.release()

    def mergeLoop(self):
        t_ind = 0
        pr_ind = 0
        global tables
        global diskManager
        global control
        self.endl = 1
        while self._running:
            if t_ind < len(tables):
                pagenum = len(tables[t_ind].pageranges)
                while pr_ind < pagenum:
                    #endl.acquire()
                    if(tables[t_ind].pageranges[pr_ind].merge_f and tables[t_ind].pageranges[pr_ind].merge()):
                        self.merge(tables[t_ind].name, tables[t_ind].pageranges[pr_ind])
                        #tables[t_ind].pageranges[pr_ind].merge_f = 0
                    pr_ind = pr_ind + 1
                    #endl.release()
                    time.sleep(0)
                pr_ind = 0
                t_ind = t_ind + 1
            else:
                time.sleep(0)
                t_ind = 0

class Database():

    def __init__(self):
        global tables
        tables = []
        #self.tables = {}  # maps {string name : Table}
        #self.num_tables = 0
        global diskManager
        diskManager = DiskManager()
        self.diskManager = diskManager
        global control
        control = BoundedSemaphore(1)
        endl = 1
        self.control = control
        self.endl = endl
        self.merger = Merger()
        self.merge_t = threading.Thread(target=self.merger.mergeLoop)
        self.merge_t.start()
        pass

    def open(self, path):
        self.diskManager.set_directory_path(path)
        pass

    def close(self):
        self.merger._running = 0
        self.merge_t.join()
        for tbl in tables:
            tbl.close()
        self.diskManager.close()
        print("database closed\n")
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if (self.diskManager.new_table_file(name, key, num_columns)):  # check if new table file was successfully created
            table = Table(name, self.diskManager, self.control, key, num_columns)
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
        if name in self.tables:
            del self.tables[name]
            self.num_tables = self.num_tables - 1
        pass

    """
    # Retruns table with the passed name
    """
    def get_table(self, name):
        #table_metadata = self.diskManager.open_table_file(name)
        #if (len(table_metadata) != 0):  # the table file and metadata exist
        table = Table(name, self.diskManager, self.control)
        if (self.diskManager.open_table_file(name, table) == None):
            return None
        else:
            tables.append(table)
            return table

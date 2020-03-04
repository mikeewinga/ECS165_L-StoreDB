from lstore.table import Table
from lstore.config import *
from lstore.index import Address
from threading import BoundedSemaphore
import threading
import time
import copy
import sys
import lstore.globals

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
        lstore.globals.control.acquire()
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
                b_pages[i+p_ind] = lstore.globals.diskManager.merge_copy_page(table, Address(page_Range.prid, 0, p_ind+i), i)
            p_ind = p_ind + step
        page_Range.merge_helper()
        page_Range.mOffSet = copy.deepcopy(page_Range.tOffSet)
        #lstore.globals.control.release()


        address = mindex.read(tid)
        t_page = address.pagenumber
        t_row = address.row

        #diskManager.debug_print_page(table, b_pages[(0, 1)])
        #look at last tail page, potentially not full
        for cur_page in range(t_page, stoper, -step):
            for recNum in range (t_row, 0, -1):
                address = Address(page_Range.prid, 1, cur_page, recNum)
                base_rid = lstore.globals.diskManager.read(table, address+BASE_RID_COLUMN)
                base_rid = int.from_bytes(base_rid, byteorder = "big")
                baddress = mindex.read(base_rid).copy()
                b = baddress
                baddress.change_flag(2)
                tSchema = lstore.globals.diskManager.read(table, address+SCHEMA_ENCODING_COLUMN)
                bSchema = lstore.globals.diskManager.read(table, baddress+SCHEMA_ENCODING_COLUMN)
                tSchema = int.from_bytes(tSchema, byteorder = "big")
                bSchema = int.from_bytes(bSchema, byteorder = "big")

                schemaToUpdate = bSchema & tSchema #bitwise AND

                resultingBaseSchema = bSchema & (~tSchema)  #bitwise AND_NOT
                # split schemaToUpdate into bool array [0,1,0,...]
                schemaToUpdate = self.getOffset(schemaToUpdate, page_Range.num_columns)
                #if cur_page == 0 and recNum == 1:
                for x in range(0, len(schemaToUpdate)):
                    if (schemaToUpdate[x]):
                        value = lstore.globals.diskManager.read(table, address+(NUM_METADATA_COLUMNS+x))
                        value = int.from_bytes(value, byteorder = "big")
                        target = baddress+(NUM_METADATA_COLUMNS+x)
                        lstore.globals.diskManager.overwrite(table, target, value)
                lstore.globals.diskManager.overwrite(table, baddress+SCHEMA_ENCODING_COLUMN, resultingBaseSchema)

            t_row = 511
        p_ind = 0
        #lstore.globals.control.acquire()
        #handle delete queue
        for rid in page_Range.delete_queue:
            address = mindex.read(rid).copy()
            address.change_flag(2)
            lstore.globals.diskManager.overwrite(table, address+RID_COLUMN, 0)
        #swap pages
        while p_ind < page_Range.bOffSet:
            for x in needs:
                address = Address(page_Range.prid, 0, p_ind+x)
                lstore.globals.diskManager.merge_replace_page(table, address)
            p_ind = p_ind + step
        while p_ind < page_Range.bOffSet:
            for i in range(page_Range.num_columns+NUM_METADATA_COLUMNS):
                lstore.globals.diskManager.overwrite(table, Address(page_Range.prid, 0, i+p_ind,0), tid)
            p_ind = p_ind + step
        page_Range.tps = tid
        lstore.globals.control.release()

    def mergeLoop(self):
        t_ind = 0
        pr_ind = 0
        while self._running:
            if t_ind < len(lstore.globals.tables):
                pagenum = len(lstore.globals.tables[t_ind].pageranges)
                while pr_ind < pagenum:
                    if(lstore.globals.tables[t_ind].pageranges[pr_ind].merge_f and lstore.globals.tables[t_ind].pageranges[pr_ind].merge()):
                        self.merge(lstore.globals.tables[t_ind].name, lstore.globals.tables[t_ind].pageranges[pr_ind])
                        #tables[t_ind].pageranges[pr_ind].merge_f = 0
                    pr_ind = pr_ind + 1
                    time.sleep(0)
                pr_ind = 0
                t_ind = t_ind + 1
            else:
                time.sleep(0)
                t_ind = 0

class Database():

    def __init__(self):
        self.merger = Merger()
        self.merge_t = threading.Thread(target=self.merger.mergeLoop)
        self.merge_t.start()

    def open(self, path):
        lstore.globals.diskManager.set_directory_path(path)

    def close(self):
        self.merger._running = 0
        self.merge_t.join()
        for tbl in lstore.globals.tables:
            tbl.close()
        lstore.globals.diskManager.close()
        print("database closed\n")

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if (lstore.globals.diskManager.new_table_file(name, key, num_columns)):  # check if new table file was successfully created
            table = Table(name, key, num_columns)
            lstore.globals.tables.append(table)
            return table
        else:
            return None

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in lstore.globals.tables:
            del lstore.globals.tables[name]

    """
    # Retruns table with the passed name
    """
    def get_table(self, name):
        table = Table(name)
        if (lstore.globals.diskManager.open_table_file(name, table) == None):
            return None
        else:
            lstore.globals.tables.append(table)
            return table

from lstore.table import Table
from lstore.disk_manager import DiskManager
from lstore.config import *
import threading
import time

def mergeLoop():
    t_ind = 0
    pr_ind = 0
    global tables
    global diskManager
    while 1:
        if t_ind < len(tables):
            pagenum = len(tables[t_ind].pageranges)
            #print(pagenum)
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
        merger = threading.Thread(target=mergeLoop)
        merger.start()
        pass

    def open(self, path):
        self.diskManager.set_directory_path(path)
        pass

    def close(self):
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
            table = Table(name, self.diskManager, key, num_columns)
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
        #table_metadata = self.diskManager.open_table_file(name)
        #if (len(table_metadata) != 0):  # the table file and metadata exist
        table = Table(name, self.diskManager)
        self.diskManager.open_table_file(name, table)
        #table.diskManager.load_pagedir_from_disk(name, table, table.pageranges, table_metadata[PRANGE_METADATA])
        tables.append(table)

        # FIXME return table
        #else:
            #return None

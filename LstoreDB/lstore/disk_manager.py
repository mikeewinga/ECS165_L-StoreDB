from collections import OrderedDict
from os import path

from lstore.config import *
from lstore.index import Address

BIN_EXTENSION = ".bin"
INDEX_EXTENSION = "_index.txt"
COLUMN_BLOCK_PAGES = 10

class Bufferpool:
    def __init__(self):
        self.max_pages = BUFFERPOOL_SIZE
        # { (string table_name, int page_range, (int base/tail, int page_num)) : Page() }
        self.page_map = OrderedDict()
        #FIXME add active tables hash set?

    def contains_page(self, table_name, address):
        if (table_name, address.page_range, address.page) in self.page_map:
            return True
        else:
            return False

    def is_full(self):
        return len(self.page_map) == BUFFERPOOL_SIZE

    """
    -maps to the actual Page in pool
    -then goes to the record offset and reads it
    """
    def read(self, table_name, address):
        page = self.page_map[(table_name, address.page_range, address.page)]
        return page.read(address.row)

    def append_write(self, table_name, address, value):
        page = self.page_map[(table_name, address.page_range, address.page)]
        page.write(value)
        page.dirty = True

    def overwrite(self, table_name, address, value):
        page = self.page_map[(table_name, address.page_range, address.page)]
        page.overwrite_record(address.row, value)
        page.dirty = True

    def delete(self, table_name, address):
        page = self.page_map[(table_name, address.page_range, address.page)]
        del self.page_map[(table_name, address.page_range, address.page)]
        return page

    """
    Delete the LRU page
    """
    def evict(self):
        # popItem pops and returns (key, value) in FIFO order with False arg
        return self.page_map.popItem(False)

    def add_page(self, table_name, address, page):
        self.page_map[(table_name, address.page_range, address.page)] = page


class DiskManager:
    """
    TO-DO list:
    -FIXME also file_offset--is this in terms of page numbers, so need to multiply by 4096 to get byte offset?
        or already in terms of bytes?
    -check that bufferpool LRU eviction thing works
    -alternative to directly modifying PageRange class: make a new wrapper Page class for Pagerange to use so that I don't have to change all the pagerange code
        in that new class, call DiskManager and pass in the page's conceptual address
        so that DiskManager can allocate new page or load into bufferpool, read, write etc.
        -the old Page class will be in-memory page to use
    -if directly modify PageRange class, then need to add page_has_capacity function for PageRange to use
    """
    def __init__(self, directory_path):
        self.bufferpool = Bufferpool()
        self.directory_path = directory_path
        self.active_table_indexes = {}  # { string table_name : {address tuple (int page_range, (int base/tail, int page_num)) : file_offset_bytes }}

    def new_table(self, table_name, total_columns):
        filename = self.directory_path + table_name + BIN_EXTENSION  # binary file for table data
        with open(filename, "x") as file: pass  # Creates file
        filename = self.directory_path + table_name + INDEX_EXTENSION  # index/config file for table
        with open(filename, "x") as file: pass

    def open_table(self, table_name):
        if path.exists(self.directory_path + table_name):
            #load the index into active_table_indexes
            self.load_index_from_disk(table_name)
            return True
        else:
            return False

    def new_page(self, table_name, total_columns, address):
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        filesize = path.getsize(filename)
        with open(filename, "r+b") as file:
            # check if last page slot isn't empty -> means the column block is full
                # and we have to allocate new column blocks
            file_offset = filesize - PAGESIZE  # get offset of last page slot
            file.seek(file_offset)
            last_page_TPS = file.read(DATASIZE)
            if (last_page_TPS != 0):
                # append whitespace for new set of column blocks to end of file here
                file.seek(filesize)
                file.write(bytearray(PAGESIZE * COLUMN_BLOCK_PAGES * total_columns))
                file_offset = filesize  # reset file_offset to start of new set of column blocks
            else:
                file_offset = filesize - (PAGESIZE * COLUMN_BLOCK_PAGES * total_columns) # get offset of first page slot in last set of column blocks
                file.seek(file_offset)
                page_TPS = file.read(DATASIZE)
                while (page_TPS != 0):  # manually search column block until empty space is found
                    file_offset += PAGESIZE
                    file.seek(file_offset)
                    page_TPS = file.read(DATASIZE)
            # write the TPS = 2^64 - 1 for first entry in the new page, for each column block
                # at the same time, add page mapping to table index
            init_TPS = 2**64 - 1
            init_TPS_bytes = init_TPS.to_bytes(8,byteorder = "big")
            table_index = self.active_table_indexes[table_name]
            for i in range(total_columns):
                file.seek(file_offset)
                file.write(init_TPS_bytes)  # write TPS number
                table_index[(address.pagerange, address.page)] = file_offset  # add mapping from conceptual address to file offset to table index
                # also load the new page into bufferpool
                in_memory_pg = Page()
                in_memory_pg.write(init_TPS_bytes)
                self.bufferpool.add_page(table_name, address, in_memory_pg)
                file_offset += (PAGESIZE * COLUMN_BLOCK_PAGES)


    def delete_page(self, table_name, base_tail, page_num):
        pass #FIXME

    def read(self, table_name, address):
        if (!self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        return self.bufferpool.read(table_name, address)

    def append_write(self, table_name, address, value):
        if (!self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.append_write(table_name, address, value)

    def overwrite(self, table_name, address, value):
        if (!self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.overwrite(table_name, address, value)

    """
    evict a page if needed before locating file in memory
    and copying needed page into the buffer pool
    """
    def load_page_from_disk(self, table_name, address):
        if (self.bufferpool.is_full()):
            # evict page and flush it to disk first
            evict_page = self.bufferpool.evict()
            self.flush_page(evict_page)

        # then locate page from disk and copy, save in Page() object, then add to buffer pool
        if table_name not in active_table_indexes:
            self.load_index_from_disk(table_name)  # load the table's index from file into memory
        table_index = self.active_table_indexes[table_name]
        file_offset = table_index[(address.pagerange, address.page)]
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        with open(filename, "rb") as file:
            file.seek(file_offset)
            page_bytes = file.read(PAGESIZE)
            page_copy = Page(page_bytes)
            self.bufferpool.add_page(table_name, address, page_copy)

    def load_index_from_disk(self, table_name):
        index_filename = self.directory_path + table_name + INDEX_EXTENSION
        self.active_table_indexes[table_name] = {}
        with open(index_filename, "r") as file:
            # split each line in file and save as key-value pairs in dictionary index
            for line in file:
                (page_range_num, base_tail, page_num, file_offset) = line.split()
                address_tuple = (int(page_range_num), (int(base_tail), int(page_num)))
                self.active_table_indexes[table_name][address_tuple] = int(file_offset)

    """
    :param evict_page: key-value pair { (string table_name, int page_range, (int base/tail, int page_num)) : Page() }
    """
    def flush_page(self, evict_page):
        table_name = evict_page[0][0]
        page_range_num = evict_page[0][1]
        page_num = evict_page[0][2]
        page = evict_page[1]
        table_index = self.active_table_indexes[table_name]
        file_offset = table_index[(page_range_num, page_num)]

        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        with open(filename, "r+b") as file:  # open file for writing without wiping the file contents
            file.seek(file_offset)
            file.write(page.data)
        pass

    def flush_index(self, table_name):
        table_index = self.active_table_indexes[table_name]
        filename = self.directory_path + table_name + INDEX_EXTENSION # file for table index
        with open(filename, "w") as file:  # open file and wipe the contents, then rewrite everything
            for address_tuple in table_index:
                index_line = str(address_tuple[0]) + " "
                    + str(address_tuple[1][0]) + " "
                    + str(address_tuple[1][1]) + " "
                    + str(table_index[address_tuple]) + "\n"
                file.write(index_line)
        del self.active_table_indexes[table_name]

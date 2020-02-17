from collections import OrderedDict
from os import path

from lstore.config import *
from lstore.index import Address

BIN_EXTENSION = ".bin"
INDEX_EXTENSION = "_index.txt"

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
    FIXME need functions that can handle new pages for different columns being created and written to disk
        this would affect the table indexes
        also involves the column-whitespace-blocking thing in the file
    """
    def __init__(self, directory_path):
        self.bufferpool = Bufferpool()
        self.directory_path = directory_path
        self.file_directory = {}  # { string table_name : int file_num }
        self.active_table_indexes = {}  # { string table_name : {address tuple (int page_range, (int base/tail, int page_num)) : physical offset in file}}

    def new_table(self, table_name):
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        with open(filename, "xb") as file: pass  # Creates file and closes it right after
        filename = self.directory_path + table_name + INDEX_EXTENSION  # index/config file for table
        with open(filename, "x") as file: pass

    def open_table(self, table_name):
        if path.exists(self.directory_path + table_name):
            #load the index into active_table_indexes
            self.load_index_from_disk(table_name)

    # def new_page_range(self, table_name, page_range):
    #     new_page = Page()
    #     bufferpool.add_page(table_name, address, new_page)
    #     # FIXME also write the entire new page range to disk

    def delete_page(self, table_name, base_tail, page_num):
        pass #FIXME

    def read(self, table_name, address):
        if (!self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        return self.bufferpool.read(table_name, base_tail, page_num, record_offset)

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
        table_index = active_table_indexes[table_name]
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
        table_index = active_table_indexes[table_name]
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


"""
-page sizes:
    -4096 bytes
    -512 record slots, each 8 bytes
-how to detect if we're writing to a newly created page that hasn't been saved in disk/bufferpool?
    -also, how to differentiate between locations of base and tail pages?
-bufferpool with max page size--in terms of physical pages
    -dictionary that maps table name + abstract page number to corresponding page in pool
    -note that there could be pages from multiple tables files in the one bufferpool
    -function that reads the page in pool
    -function that writes to the page in pool (need to add bool to page.py to mark if it's dirty)
        -also need both write and overwrite funcs
    -function to copy in new page and find area to overwrite during evictions at the granularity of entire base page
    -variable tracking the LRU/MRU pages?
-disk_manager class:
    -instance of bufferpool object
    -dictionary file directory that maps table names to the file on disk
    -function that copies the needed page from file into bufferpool
    -function that evicts page from bufferpool and flushes it to file if dirty
    -function that takes in table name and page number, and maps to either:
        -the page in bufferpool
        -the physical page on file (and brings it into memory)
    -functions to read/write the pages in bufferpool
    -deleting entire table or physical pages?
-each table has corresponding file, and the file stores pages in sets of multiple page ranges
    -10 page ranges in 10,000 record table
    -4 KB per loadable chunk of memory
    -format it so that seeking is easy
    -will want to convert into bytearray
-file manipulation:
    -opening file just gets you file pointer
    -any operation on file will load file into memory, but if you only want to partially load the
    file, then use process(line, file seek(), chunk = infile.read(chunksize) etc. for partial loading into RAM
        -https://stackoverflow.com/questions/6475328/how-can-i-read-large-text-files-in-python-line-by-line-without-loading-it-into
-later on, may need to change table.py functions to integrate the disk manager
    (but the Index page directory in table.py shouldn't change)
-Page() is in-memory, maybe need a converter to convert Page() to file storage and parse vice versa
"""

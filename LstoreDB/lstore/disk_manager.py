from collections import OrderedDict

class Bufferpool:
    def __init__(self, max_pages):
        self.max_pages = max_pages
        self.page_map = OrderedDict()  # { (string table_name, int base/tail, int page_num) : Page() }

    def contains_page(self, table_name, base_tail, page_num):
        if (table_name, base_tail, page_num) in self.page_map:
            return True
        else:
            return False

    def is_full(self):
        return len(self.page_map) == self.max_pages

    """
    -maps to the actual Page in pool
    -then goes to the record offset and reads it
    """
    def read(self, table_name, base_tail, page_num, record_offset):
        page = self.page_map[(table_name, base_tail, page_num)]
        return page.read(record_offset)

    def append_write(self, table_name, base_tail, page_num, value):
        page = self.page_map[(table_name, base_tail, page_num)]
        page.write(value)
        page.dirty = True

    def overwrite(self, table_name, base_tail, page_num, record_offset, value):
        page = self.page_map[(table_name, base_tail, page_num)]
        page.overwrite_record(record_offset, value)
        page.dirty = True

    def delete(self, table_name, base_tail, page_num):
        page = self.page_map[(table_name, base_tail, page_num)]
        del self.page_map[(table_name, base_tail, page_num)]
        return page

    """
    Delete the LRU page
    """
    def evict(self):
        # popItem pops and returns (key, value) in FIFO order with False arg
        # return the page in (key, value) pair
        return self.page_map.popItem(False)[1]

    def add_page(self, table_name, base_tail, page_num, page):
        self.page_map[(table_name, base_tail, page_num)] = page

class DiskManager:
    def __init__(self, max_buffer_pages):
        self.bufferpool = Bufferpool(max_buffer_pages)
        self.file_directory = {}  # { string table_name : int file_num }

    def new_table(self, table_name):
        filename = table_name + ".dat"
        with open(filename, "rb+") as file: pass  # Creates file and closes it right after

    def create_new_page(self, table_name, base_tail, page_num):
        new_page = Page(True)  # mark new page as dirty so when it's evicted from bufferpool, it will be flushed to disk
        bufferpool.add_page(table_name, base_tail, page_num, new_page)

    def delete_page(self, table_name, base_tail, page_num):
        pass #FIXME

    def read(self, table_name, base_tail, page_num, record_offset):
        if (!self.bufferpool.contains_page(table_name, base_tail, page_num)):
            load_from_disk(table_name, base_tail, page_num)
        return self.bufferpool.read(table_name, base_tail, page_num, record_offset)

    def append_write(self, table_name, base_tail, page_num, value):
        if (!self.bufferpool.contains_page(table_name, base_tail, page_num)):
            load_from_disk(table_name, base_tail, page_num)
        self.bufferpool.append_write(table_name, base_tail, page_num, value)

    def overwrite(self, table_name, base_tail, page_num, record_offset, value):
        if (!self.bufferpool.contains_page(table_name, base_tail, page_num)):
            load_from_disk(table_name, base_tail, page_num)
        self.bufferpool.overwrite(table_name, base_tail, page_num, record_offset, value)

    """
    evict a page if needed before locating file in memory
    and copying needed page into the buffer pool
    """
    def load_from_disk(self, table_name, base_tail, page_num):
        if (self.bufferpool.is_full()):
            pass
            # evict page and flush it to disk first
        # then locate page from disk and copy, save in Page() object, then add to buffer pool
        pass

    def flush_page(self, page):
        pass


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

from collections import OrderedDict
import os

from lstore.config import *
#from lstore.index import Address
from lstore.page import Page

BIN_EXTENSION = ".bin"
INDEX_EXTENSION = "_index.txt"
COLUMN_BLOCK_BYTES = PAGESIZE * COLUMN_BLOCK_PAGES

class Bufferpool:
    def __init__(self):
        self.max_pages = BUFFERPOOL_SIZE
        # { (string table_name, int page_range, (int base/tail, int page_num)) : Page() }
        self.page_map = OrderedDict()

    def contains_page(self, table_name, address):
        if (table_name, address.page_range, address.page) in self.page_map:
            return True
        else:
            return False

    def is_full(self):
        return len(self.page_map) == BUFFERPOOL_SIZE

    def is_empty(self):
        return len(self.page_map) == 0

    """
    -maps to the actual Page in pool
    -then goes to the record offset and reads it
    """
    def read(self, table_name, address):
        self.pin_page(table_name, address)  # note that this would be critical section in multithreading
        page = self.page_map[(table_name, address.page_range, address.page)]
        self.page_map.move_to_end((table_name, address.page_range, address.page))
        read_value = page.read(address.row)
        self.unpin_page(table_name, address)
        return read_value

    def append_write(self, table_name, address, value):
        self.pin_page(table_name, address)  # note that this would be critical section in multithreading
        page = self.page_map[(table_name, address.page_range, address.page)]
        page.write(value)
        page.dirty = True
        self.page_map.move_to_end((table_name, address.page_range, address.page))
        self.unpin_page(table_name, address)

    def overwrite(self, table_name, address, value):
        self.pin_page(table_name, address)  # note that this would be critical section in multithreading
        page = self.page_map[(table_name, address.page_range, address.page)]
        page.overwrite_record(address.row, value)
        page.dirty = True
        self.page_map.move_to_end((table_name, address.page_range, address.page))
        self.unpin_page(table_name, address)

    def page_has_capacity(self, table_name, address):
        return self.page_map[(table_name, address.page_range, address.page)].has_capacity()

    """
    FIXME do we need this function?
    """
    def delete(self, table_name, address):
        page = self.page_map[(table_name, address.page_range, address.page)]
        del self.page_map[(table_name, address.page_range, address.page)]
        return page

    """
    Delete the LRU page
    """
    def evict(self):
        # popItem pops and returns (key, value) in FIFO order with False arg
        address_to_page = self.page_map.popItem(False)
        table_name = address_to_page[0][0]
        page_range_num = address_to_page[0][1]
        page_num = address_to_page[0][2]
        page = address_to_page[1]
        if (page.pin_count > 0):  # page is in use
            self.page_map[(table_name, page_range_num, page_num)] = page  # re-add the page back into dictionary
            return ()
        else:
            return address_to_page

    def add_page(self, table_name, address, page):
        self.page_map[(table_name, address.page_range, address.page)] = page
        # self.page_map.move_to_end((table_name, address.page_range, address.page))

    def pin_page(self, table_name, address):
        self.page_map[(table_name, address.page_range, address.page)].pin_count += 1

    def unpin_page(self, table_name, address):
        self.page_map[(table_name, address.page_range, address.page)].pin_count -= 1


class DiskManager:
    def __init__(self):
        self.bufferpool = Bufferpool()
        self.directory_path = ""
        self.active_table_indexes = {}  # { string table_name : {address tuple (int page_range, (int base/tail, int page_num)) : (file_offset_bytes, num_records) }}
        self.active_table_metadata = {}  # { string table_name: (int primary key index, int num_total_columns) }

    def set_directory_path(self, directory_path):
        self.directory_path = directory_path + "/"
        try:
            os.makedirs(directory_path)
        except OSError:
            print("Creation of the directory %s failed" % directory_path)
        else:
            print("Successfully created the directory %s " % directory_path)

    """
    :param primary_key: index of primary user column
    """
    def new_table_file(self, table_name, primary_key, num_user_columns):
        try:
            filename = self.directory_path + table_name + BIN_EXTENSION  # binary file for table data
            with open(filename, "x") as file: pass  # Creates file
            filename = self.directory_path + table_name + INDEX_EXTENSION  # index/config file for table
            with open(filename, "x") as file:
                file.write(str(primary_key) + "\n")  # save primary key column number
                file.write(str(num_user_columns + NUM_METADATA_COLUMNS) + "\n")  # save total number of columns
            self.active_table_metadata[table_name] = (primary_key, num_user_columns + NUM_METADATA_COLUMNS)
            self.active_table_indexes[table_name] = {}
            return True
        except FileExistsError:
            return False

    def open_table_file(self, table_name):
        if os.path.exists(self.directory_path + table_name + INDEX_EXTENSION)\
                and os.path.exists(self.directory_path + table_name + BIN_EXTENSION):
            #load the index into active_table_indexes
            self.load_index_from_disk(table_name)
            return self.active_table_metadata[table_name]
        else:
            return ()

    def new_page(self, table_name, address, column_index):
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        orig_filesize = os.path.getsize(filename)
        total_columns = self.active_table_metadata[table_name][COLUMNS]
        column_set_size = COLUMN_BLOCK_BYTES * total_columns
        file_offset = orig_filesize - column_set_size + (COLUMN_BLOCK_BYTES * column_index)

        with open(filename, "r+b") as file:
            block_full = True
            for i in range(COLUMN_BLOCK_PAGES):
                file.seek(file_offset)
                page_TPS = file.read(DATASIZE)
                if (page_TPS == 0):
                    block_full = False
                    break
                file_offset += PAGESIZE
            if (block_full):
                # append whitespace for new set of column blocks to end of file here
                file.seek(orig_filesize)
                file.write(bytearray(column_set_size))
                file_offset = orig_filesize + (COLUMN_BLOCK_BYTES * column_index)  # reset file_offset to start of column block to modify
            # write the TPS = 2^64 - 1 for first entry in the new page, for each column block
                # at the same time, add page mapping to table index
            init_TPS = 2**64 - 1
            init_TPS_bytes = init_TPS.to_bytes(8,byteorder = "big")
            table_index = self.active_table_indexes[table_name]
            file.seek(file_offset)  # reset file position to start of blank page slot
            file.write(init_TPS_bytes)  # write TPS number
            table_index[(address.pagerange, address.page)] = (file_offset, 0)  # add to table index the mapping from conceptual address to file offset + num_records
            # also load the new page into bufferpool
            in_memory_pg = Page()
            self.bufferpool.add_page(table_name, address, in_memory_pg)

    def delete_page(self, table_name, base_tail, page_num):
        pass #FIXME set TPS to 1 for the page to mark as deleted

    def read(self, table_name, address):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        return self.bufferpool.read(table_name, address)

    def append_write(self, table_name, address, value):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.append_write(table_name, address, value)
        table_index = self.active_table_indexes[table_name]
        table_index[(address.pagerange, address.page)][NUM_RECORDS] += 1

    def overwrite(self, table_name, address, value):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.overwrite(table_name, address, value)

    def page_has_capacity(self, table_name, address):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        return self.bufferpool.page_has_capacity(table_name, address)

    def page_num_records(self, table_name, address):
        table_index = self.active_table_indexes[table_name]
        num_records = table_index[(address.pagerange, address.page)][NUM_RECORDS]
        return num_records

    """
    evict a page if needed before locating file in memory
    and copying needed page into the buffer pool
    """
    def load_page_from_disk(self, table_name, address):
        if (self.bufferpool.is_full()):
            # evict page and flush it to disk if dirty
            evict_page = self.bufferpool.evict()
            while (len(evict_page) == 0):
                evict_page = self.bufferpool.evict()
            if (evict_page[1].dirty):
                self.flush_page(evict_page)

        # then locate page from disk and copy, save in Page() object, then add to buffer pool
        if table_name not in self.active_table_indexes:
            self.load_index_from_disk(table_name)  # load the table's index from file into memory
        table_index = self.active_table_indexes[table_name]
        file_offset = table_index[(address.pagerange, address.page)][FILE_OFFSET]
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
            # read primary key index and num columns metadata
            primary_key = next(file)
            num_total_columns = next(file)
            self.active_table_metadata[table_name] = (primary_key, num_total_columns)
            # split each line in file and save as key-value pairs in dictionary index
            for line in file:
                (page_range_num, base_tail, page_num, file_offset, num_records) = line.split()
                address_tuple = (int(page_range_num), (int(base_tail), int(page_num)))
                self.active_table_indexes[table_name][address_tuple] = (int(file_offset), int(num_records))

    """
    :param evict_page: key-value pair { (string table_name, int page_range, (int base/tail, int page_num)) : Page() }
    """
    def flush_page(self, evict_page):
        table_name = evict_page[0][0]
        page_range_num = evict_page[0][1]
        page_num = evict_page[0][2]
        page = evict_page[1]
        table_index = self.active_table_indexes[table_name]
        file_offset = table_index[(page_range_num, page_num)][FILE_OFFSET]

        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        with open(filename, "r+b") as file:  # open file for writing without wiping the file contents
            file.seek(file_offset)
            file.write(page.data)

    """
    note that this function doesn't delete the entry from dictionary, just flushes it to disk
    """
    def flush_index_metadata(self, table_name):
        table_metadata = self.active_table_metadata[table_name]
        table_index = self.active_table_indexes[table_name]
        filename = self.directory_path + table_name + INDEX_EXTENSION # file for table index
        with open(filename, "w") as file:  # open file and wipe the contents, then rewrite everything
            # copy the metadata into file
            file.write(str(table_metadata[PRIMARY_KEY]) + "\n")
            file.write(str(table_metadata[COLUMNS]) + "\n")
            # copy the index into file
            for address_tuple in table_index:
                index_line = str(address_tuple[0]) + " "\
                             + str(address_tuple[1][0]) + " "\
                             + str(address_tuple[1][1]) + " "\
                             + str(table_index[address_tuple][FILE_OFFSET]) + " "\
                             + str(table_index[address_tuple][NUM_RECORDS]) + "\n"
                file.write(index_line)
        # del self.active_table_indexes[table_name]

    def close(self):
        # empty the bufferpool
        while (not self.bufferpool.is_empty()):
            # evict page and flush it to disk if dirty
            evict_page = self.bufferpool.evict()
            if (evict_page[1].dirty):
                self.flush_page(evict_page)
        # flush the table indexes to config files and then clear dictionary of indexes
        for table_name in self.active_table_indexes.keys():
            self.flush_index_metadata(table_name)
        self.active_table_indexes.clear()

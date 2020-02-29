from lstore.page import Page
from lstore.index import Address
from lstore.pagerange import PageRange
from lstore.table import *
from lstore.config import *
from collections import OrderedDict
import os

class Bufferpool:
    def __init__(self):
        self.max_pages = BUFFERPOOL_SIZE
        # { (string table_name, int page_range, (int base/tail, int page_num)) : Page() }
        self.page_map = OrderedDict()

    def contains_page(self, table_name, address):
        if (table_name, address.pagerange, address.page) in self.page_map:
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
        page = self.page_map[(table_name, address.pagerange, address.page)]
        self.page_map.move_to_end((table_name, address.pagerange, address.page))
        read_value = page.read(address.row)
        self.unpin_page(table_name, address)
        return read_value

    def append_write(self, table_name, address, value):
        self.pin_page(table_name, address)  # note that this would be critical section in multithreading
        page = self.page_map[(table_name, address.pagerange, address.page)]
        page.write(value)
        page.dirty = True
        self.page_map.move_to_end((table_name, address.pagerange, address.page))
        self.unpin_page(table_name, address)

    def overwrite(self, table_name, address, value):
        self.pin_page(table_name, address)  # note that this would be critical section in multithreading
        page = self.page_map[(table_name, address.pagerange, address.page)]
        page.overwrite_record(address.row, value)
        page.dirty = True
        self.page_map.move_to_end((table_name, address.pagerange, address.page))
        self.unpin_page(table_name, address)

    """
    param address: the Address object of the original base page to copy (flag = 0)
    """
    def merge_copy_page(self, table_name, address):
        page = self.page_map[(table_name, address.pagerange, address.page)]
        new_page = page.copy()
        new_page.unpin()
        # change the base/tail flag to 2, so address refers to merge base page
        self.page_map[(table_name, address.pagerange, (2, address.pagenumber))] = new_page

    """
    param address: the original base page (flag = 0)
    """
    def merge_replace_page(self, table_name, address):
        orig_page = self.page_map[(table_name, address.pagerange, address.page)]
        orig_page.pin()
        merge_address = address.copy()
        merge_address.change_flag(2)  # change the base/tail flag to 2, so address refers to merge base page
        merge_page = self.page_map[(table_name, merge_address.pagerange, merge_address.page)]
        merge_page.pin()
        # delete the entry for merge page address from page_map
        del self.page_map[(table_name, merge_address.pagerange, merge_address.page)]
        # replace the Page() for original page address with the merge_page
        self.page_map[(table_name, address.pagerange, address.page)] = merge_page
        orig_page.unpin()
        merge_page.unpin()

    def page_has_capacity(self, table_name, address):
        return self.page_map[(table_name, address.pagerange, address.page)].has_capacity()

    """
    FIXME do we need this function?
    """
    def delete(self, table_name, address):
        page = self.page_map[(table_name, address.pagerange, address.page)]
        del self.page_map[(table_name, address.pagerange, address.page)]
        return page

    """
    Delete the LRU page
    """
    def evict(self):
        # popItem pops and returns (key, value) in FIFO order with False arg
        address_to_page = self.page_map.popitem(False)
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
        self.page_map[(table_name, address.pagerange, address.page)] = page
        # self.page_map.move_to_end((table_name, address.pagerange, address.page))

    def pin_page(self, table_name, address):
        #self.page_map[(table_name, address.pagerange, address.page)].pin_count += 1
        self.page_map[(table_name, address.pagerange, address.page)].pin()

    def unpin_page(self, table_name, address):
        #self.page_map[(table_name, address.pagerange, address.page)].pin_count -= 1
        self.page_map[(table_name, address.pagerange, address.page)].unpin()


class DiskManager:
    def __init__(self):
        self.bufferpool = Bufferpool()
        self.directory_path = ""
        self.active_table_indexes = {}  # { string table_name : {address tuple (int page_range, (int base/tail, int page_num)) : [file_offset_bytes, num_records] }}
        self.active_table_metadata = {}  # { string table_name: (int primary key index, int num_user_columns, current_rid_base, current_rid_tail, current_prid, {prid : (bOffset, tOffset, cur_tid, mOffset, merge_f)} }

    def set_directory_path(self, directory_path):
        self.directory_path = directory_path + "/"
        try:
            os.makedirs(directory_path)
        except OSError:
            if(os.path.exists(directory_path)):
                return
            else:
                print("Creation of the directory %s failed" % directory_path)
        else:
            print("Successfully created the directory %s " % directory_path)

    """
    :param primary_key: index of primary user column
    """
    def new_table_file(self, table_name, primary_key, num_user_columns):
        try:
            filename = self.directory_path + table_name + BIN_EXTENSION  # binary file for table data
            with open(filename, "xb") as file:  # Creates file
                # allocate whitespace for first set of column blocks
                column_set_size = COLUMN_BLOCK_BYTES * (num_user_columns + NUM_METADATA_COLUMNS)
                file.write(bytearray(column_set_size))
            filename = self.directory_path + table_name + INDEX_EXTENSION  # index/config file for table
            with open(filename, "x") as file:
                file.write(str(primary_key) + "\n")  # save primary key column number
                file.write(str(num_user_columns) + "\n")  # save number of user columns
            # add entries to the metadata dictionaries for the new table
            self.active_table_metadata[table_name] = (primary_key, num_user_columns)
            self.active_table_indexes[table_name] = {}
            filename = self.directory_path + table_name + PAGE_DIR_EXTENSION  # page directory config file
            with open(filename, "x") as file: pass
            return True
        except FileExistsError:
            return False

    def open_table_file(self, table_name, table):
        if not (os.path.exists(self.directory_path + table_name + INDEX_EXTENSION)
                and os.path.exists(self.directory_path + table_name + BIN_EXTENSION)
                and os.path.exists(self.directory_path + table_name + PAGE_DIR_EXTENSION)):
            return None
        # else the files exist
        # load the index into active_table_indexes
        self.load_index_from_disk(table_name)
        table_metadata = self.active_table_metadata[table_name]
        table.set_table_metadata(table_metadata[PRIMARY_KEY], table_metadata[COLUMNS], table_metadata[BASE_RID], table_metadata[TAIL_RID], table_metadata[PRID])
        self.load_pagedir_from_disk(table_name, table)
        return table

    def new_page(self, table_name, address, column_index):
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        orig_filesize = os.path.getsize(filename)
        total_columns = int(self.active_table_metadata[table_name][COLUMNS]) + NUM_METADATA_COLUMNS
        column_set_size = COLUMN_BLOCK_BYTES * total_columns
        file_offset = orig_filesize - column_set_size + (COLUMN_BLOCK_BYTES * column_index)

        with open(filename, "r+b") as file:
            block_full = True
            for i in range(COLUMN_BLOCK_PAGES):
                file.seek(file_offset)
                page_TPS = int.from_bytes(file.read(DATASIZE), byteorder='big', signed=False)
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
            # add to table index the mapping from conceptual address to file offset + num_records
            # -- note that num_records is initially 1 because the TPS is first data entry
            table_index[(address.pagerange, address.page)] = [file_offset, 1]
            # also load the new page into bufferpool, checking first if bufferpool needs to evict a page
            if (self.bufferpool.is_full()):
                # evict page and flush it to disk if dirty
                evict_page = self.bufferpool.evict()
                while (len(evict_page) == 0):
                    evict_page = self.bufferpool.evict()
                if (evict_page[1].dirty):
                    self.flush_page(evict_page)
            in_memory_pg = Page()
            self.bufferpool.add_page(table_name, address, in_memory_pg)
        return file_offset

    """
    param address: address of original base page, flag = 0
    """
    def merge_copy_page(self, table_name, address, column_index):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.pin_page(table_name, address)
        # call on bufferpool to copy the page for the merge thread, checking first if bufferpool needs to evict page
        if (self.bufferpool.is_full()):
            # evict page and flush it to disk if dirty
            evict_page = self.bufferpool.evict()
            while (len(evict_page) == 0):
                evict_page = self.bufferpool.evict()
            if (evict_page[1].dirty):
                self.flush_page(evict_page)
        merge_page_address = address.copy()
        merge_page_address.change_flag(2)
        # allocate a new page in file and save the physical file offset in table_index
        file_offset = self.new_page(table_name, merge_page_address, column_index)
        table_index = self.active_table_indexes[table_name]
        # change the base/tail flag to 2, so address refers to merge base page
        table_index[(merge_page_address.pagerange, merge_page_address.page)] = [file_offset, 1]
        self.bufferpool.merge_copy_page(table_name, address)
        self.bufferpool.unpin_page(table_name, address)
        return merge_page_address

    """
    param address: the original base page (flag = 0)
    """
    def merge_replace_page(self, table_name, address):
        if (not self.bufferpool.contains_page(table_name, address)):
            self.load_page_from_disk(table_name, address)
        self.bufferpool.merge_replace_page(table_name, address)
        merge_address = address.copy()
        merge_address.change_flag(2)
        table_index = self.active_table_indexes[table_name]
        # grab the file offset mapped to the merge page address
        new_file_offset = table_index[(merge_address.pagerange, merge_address.page)][FILE_OFFSET]
        # replace the old file offset for the original address with new offset
        table_index[(address.pagerange, address.page)][FILE_OFFSET] = new_file_offset
        # delete merge page address entry from table_index
        del table_index[(merge_address.pagerange, merge_address.page)]

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
        num_records = table_index[(address.pagerange, address.page)][NUM_RECORDS]
        filename = self.directory_path + table_name + BIN_EXTENSION  # file for table data
        with open(filename, "rb") as file:
            file.seek(file_offset)
            page_bytes = file.read(PAGESIZE)
            page_copy = Page(page_bytes, num_records)
            #page_copy = Page(page_bytes)
            self.bufferpool.add_page(table_name, address, page_copy)

    def load_index_from_disk(self, table_name):
        index_filename = self.directory_path + table_name + INDEX_EXTENSION
        self.active_table_indexes[table_name] = {}
        with open(index_filename, "r") as file:
            # read primary key index and num columns metadata
            metadata_line = next(file)
            (primary_key, num_user_columns, current_rid_base, current_rid_tail, current_prid) = map(int, metadata_line.split())
            pRange_metadata = {}
            for i in range(current_prid+1):  # for all the page ranges
                pagerange_data = next(file)
                pagerange_number, bOffset, tOffset, cur_tid, mOffset, merge_f = map(int, pagerange_data.split())
                pRange_metadata[pagerange_number] = (bOffset, tOffset, cur_tid, mOffset, merge_f)
            self.active_table_metadata[table_name] = (primary_key, num_user_columns, current_rid_base, current_rid_tail, current_prid, pRange_metadata)
            # split each line in file and save as key-value pairs in dictionary index
            for line in file:
                page_range_num, base_tail, page_num, file_offset, num_records = map(int, line.split())
                address_tuple = (page_range_num, (base_tail, page_num))
                self.active_table_indexes[table_name][address_tuple] = [file_offset, num_records]

    def load_pagedir_from_disk(self, table_name, table):
        # call on table to create new page ranges and set each range's metadata
        table_metadata = self.active_table_metadata[table_name]
        num_page_ranges = table_metadata[PRID] + 1
        prange_metadata = table_metadata[PRANGE_METADATA]
        for prid in range(num_page_ranges):
            table.add_page_range(prid, prange_metadata[prid])
        # read in page directory and add into table's and page range's page directories
        dir_file = self.directory_path + table_name + PAGE_DIR_EXTENSION
        with open(dir_file, "r") as file:
            for line in file:
                rid, pagerange_num, flag, pagenumber, row = map(int, line.split())
                address = Address(pagerange_num, flag, pagenumber, row)
                table.add_pagedir_entry(rid, pagerange_num)
                table.get_page_range(pagerange_num).add_pagedir_entry(rid, address)

    # def load_pagedir_from_disk(self, table_name, table_class, pagerange_class, pagerange_metadata):
    #     dir_file = self.directory_path + table_name + PAGE_DIR_EXTENSION
    #     pagerange_class[0].bOffSet = pagerange_metadata[0][BOFFSET]
    #     pagerange_class[0].tOffSet = pagerange_metadata[0][TOFFSET]
    #     with open(dir_file, "r") as file:
    #         for line in file:
    #             rid, pagerange, flag, pagenumber, row = map(int, line.split())
    #             table_class.index.write(rid, pagerange)
    #             # if there is more than one page range, allocate more pageranges
    #             if(len(pagerange_class) <= pagerange ):
    #                 pagerange_class[pagerange] = PageRange(table_class.name, pagerange, table_class.current_Rid_base, table_class.num_columns, table_class.diskManager, pagerange_metadata[pagerange][BOFFSET], pagerange_metadata[pagerange][TOFFSET])
    #             pagerange_class[pagerange].index.write(rid, Address(pagerange, flag, pagenumber, row))


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

    def flush_table_metadata(self, table_name, current_rid_base, current_rid_tail, cur_prid):
        table_metadata = self.active_table_metadata[table_name]
        filename = self.directory_path + table_name + INDEX_EXTENSION # file for table index
        with open(filename, "w") as file:  # open file and wipe the contents, then rewrite everything
            # copy the metadata into file
            metadata_line = str(table_metadata[PRIMARY_KEY]) + " " \
                            + str(table_metadata[COLUMNS]) + " " \
                            + str(current_rid_base) + " " \
                            + str(current_rid_tail) + " " \
                            + str(cur_prid) + "\n"
            file.write(metadata_line)

    """
    :param metadata_dict: { int prid : (int bOffset, int tOffset, int cur_tid, int mOffset, int merge_f) }
    """
    def flush_pagerange_metadata(self, table_name, metadata_dict):
        filename = self.directory_path + table_name + INDEX_EXTENSION # file for table index
        with open(filename, "a") as file:
            for prid in metadata_dict:
                page_range_line = str(prid) + " " \
                                  + str(metadata_dict[prid][BOFFSET]) + " " \
                                  + str(metadata_dict[prid][TOFFSET]) + " " \
                                  + str(metadata_dict[prid][CUR_TID]) + " " \
                                  + str(metadata_dict[prid][MOFFSET]) + " " \
                                  + str(metadata_dict[prid][MERGE_F]) + "\n"
                file.write(page_range_line)

    """
    note that this function doesn't delete the entry from dictionary, just flushes it to disk
    """
    def flush_index(self, table_name, current_rid_base, current_rid_tail, cur_prid):
        table_index = self.active_table_indexes[table_name]
        filename = self.directory_path + table_name + INDEX_EXTENSION # file for table index
        with open(filename, "a") as file:  # open file and append
            # copy the index into file
            for address_tuple in table_index:
                index_line = str(address_tuple[0]) + " "\
                             + str(address_tuple[1][0]) + " "\
                             + str(address_tuple[1][1]) + " "\
                             + str(table_index[address_tuple][FILE_OFFSET]) + " "\
                             + str(table_index[address_tuple][NUM_RECORDS]) + "\n"
                file.write(index_line)
        # del self.active_table_indexes[table_name]

    def flush_page_directory(self, table_name, pagedir_dict):
        filename = self.directory_path + table_name + PAGE_DIR_EXTENSION # file for page directory
        with open(filename, "w") as file:
            for rid, address in pagedir_dict.items():
                dir_line = str(rid) + " " \
                             + str(address.pagerange) + " " \
                             + str(address.flag) + " " \
                             + str(address.pagenumber) + " " \
                             + str(address.row) + "\n"
                file.write(dir_line)

    def close(self):
        # empty the bufferpool
        while (not self.bufferpool.is_empty()):
            # evict page and flush it to disk if dirty
            evict_page = self.bufferpool.evict()
            if (evict_page[1].dirty):
                self.flush_page(evict_page)
        # flush the table indexes to config files and then clear dictionary of indexes
        # for table_name in self.active_table_indexes.keys():
        #     self.flush_index_metadata(table_name)
        self.active_table_indexes.clear()

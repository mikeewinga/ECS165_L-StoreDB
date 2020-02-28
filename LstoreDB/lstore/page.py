from lstore.config import *

class Page:
    
    """
    Creates a page of size bytes and with number of records num_records.
    If bytes is None, creates a new page of size PAGESIZE with new tps
    """
    def __init__(self, bytes = None, num_records = 0):
        self.num_records = num_records
        self.dirty = False
        self.pin_count = 0
        if (bytes != None):
            self.data = bytearray(bytes)
        else:
            self.data = bytearray(PAGESIZE)
            self.write(2**64 - 1)  # fill up first record slot with TPS number if the Page() is created from scratch

    """
    Returns a copy of the page
    """
    def copy(self):
        copy_page = Page(self.data, self.num_records)
        copy_page.dirty = self.dirty
        copy_page.pin_count = self.pin_count
        return copy_page

    """
    Pins the page
    """
    def pin(self):
        self.pin_count += 1

    """
    Unpins the page
    """
    def unpin(self):
        self.pin_count -= 1

    """
    Checks if there is space left in page
    Returns True or False
    """
    def has_capacity(self):
        return self.num_records < PAGESIZE/DATASIZE

    """
    Converts a value that may be string, int, etc. to bytes (size 8)
    """
    def convert_to_bytes(self, value):
        if (isinstance(value, str)):
            insert = bytes(value, 'utf-8')
        elif (isinstance(value, int)):
            insert = value.to_bytes(8,byteorder = "big")
        elif (value == None):
            insert = bytearray(8)
        else:
            insert = bytes(value)
        return insert

    """
    Appends a new value at the first empty entry in page
    """
    def write(self, value):
        if (self.has_capacity()):
            insert = self.convert_to_bytes(value)
            # Find position of empty space and insert data at that index
            pos = self.num_records * DATASIZE
            self.data[pos:pos+len(insert)] = insert
            self.num_records += 1
            self.dirty = True
            return True
        return False

    """
    Replaces current value at record_index with new given value 
    """
    def overwrite_record(self, record_index, value):
        if (record_index < PAGESIZE/DATASIZE):
            insert = self.convert_to_bytes(value)
            # Find byte position corresponding to record and overwrite the data
            pos = record_index * DATASIZE
            self.data[pos:pos+(DATASIZE)] = insert
            self.dirty = True
            return True
        return False

    """
    Reads the value at the given index
    :return: bytearray of 8 bytes
    """
    def read(self, index):
        if (index < PAGESIZE/DATASIZE):
            # Find byte position corresponding to record and read the data
            pos = index * DATASIZE
            value = self.data[pos:pos+(DATASIZE)]
            return value
            #print("".join("\\x%02x" % i for i in value))

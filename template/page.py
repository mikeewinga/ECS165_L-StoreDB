from config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGESIZE)

    def has_capacity(self):
        return self.num_records < PAGESIZE/DATASIZE

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

    def write(self, value):
        if (self.has_capacity()):
            insert = convert_to_bytes(value)
            pos = self.num_records * DATASIZE
            self.data[pos:pos+len(insert)] = insert
            self.num_records += 1
            return True
        return False

    def overwrite_record(self, record_index, value):
        if (record_index < PAGESIZE/DATASIZE):
            insert = convert_to_bytes(value)
            pos = record_index * DATASIZE
            self.data[pos:pos+(DATASIZE)] = insert
            return True
        return False

    def read(self, index):
        if (index < PAGESIZE/DATASIZE):
            pos = index * DATASIZE
            value = self.data[pos:pos+(DATASIZE)]
            return value
            #print("".join("\\x%02x" % i for i in value))

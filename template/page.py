from config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGESIZE)

    def has_capacity(self):
        return self.num_records < PAGESIZE/DATASIZE

    def write(self, value):
        if (self.has_capacity()):
            if (isinstance(value, str)):
                insert = bytes(value, 'utf-8')
            elif (isinstance(value, int)):
                insert = value.to_bytes(8,byteorder = "big")
            else:
                insert = bytes(value)
            pos = self.num_records * DATASIZE
            self.data[pos:pos+len(insert)] = insert
            self.num_records += 1
            return True
        return False
        
    def read(self, index):
        if (index < PAGESIZE/DATASIZE):
            pos = index * DATASIZE
            value = self.data[pos:pos+(DATASIZE)]
            return value
            #print("".join("\\x%02x" % i for i in value))


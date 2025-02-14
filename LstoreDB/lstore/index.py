from lstore.config import *
from lstore.address import Address
import lstore.globals
from lstore.latch import Latch

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

class Index:

    def __init__(self, table):
        self.table = table
        #self.indexDict = [{}]*table.num_columns
        self.indexDict = []
            
    def set_width(self, num_columns):
        self.hasIndex = [0]*num_columns
        #print(num_columns)
        for i in range(num_columns):
            self.indexDict.append({})  # each dictionary maps {key value : list of RID's}

    """
    # returns the location of all records with the given value
    # i.e. an RID of base page
    :param value: user-given key
    :return: list of RID's
    """
    def locate(self, column, value):
        lstore.globals.icon.latch()
        #intList = [] # saves the RID's of matching records
        if self.indexDict[column].get(value):  # check if given key exists in indexDict
            ridList = self.indexDict[column][value]
            # convert byte format to RID numbers
            #for x in byteList:
                #intList.append(x)
            lstore.globals.icon.unlatch()
            return ridList
        else:
            lstore.globals.icon.unlatch()
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        pass

    def update(self, rid, original, *input):
        lstore.globals.icon.latch()
        for i in range(0, len(input)):
            if input[i]:
                if self.hasIndex[i]:
                    if self.indexDict[i].get(original[i]):
                        byteList = self.indexDict[i][original[i]]
                        byteList.remove(rid)
                    F = self.indexDict[i].get(input[i])
                    if F:
                        F.append(rid)
                    else:
                        self.indexDict[i][input[i]] = [rid]
        lstore.globals.icon.unlatch()

    """
    # optional: Create index on specific column
    :param column_number: int
    """

    def create_index(self, column_number):
        lstore.globals.icon.latch()
        if self.hasIndex[column_number]:
            lstore.globals.icon.unlatch()
            return
        else:
            self.hasIndex[column_number] = 1
        # number of page ranges needed for index
        numIndexPages = self.table.current_Rid_base // RANGESIZE
        # for every record, map the key of given column number to RID and save in dictionary
        step = NUM_METADATA_COLUMNS + self.table.num_columns
        query_columns = [0] * self.table.num_columns
        query_columns[column_number] = 1
        for i in range(0, numIndexPages+1):  # for all page ranges in table
            for j in range(0,self.table.pageranges[i].bOffSet+RID_COLUMN,step):  # for all base physical RID pages in range
                rid_page_address = Address(i, 0, RID_COLUMN+j)
                num_records = lstore.globals.diskManager.page_num_records(self.table.name, rid_page_address)
                for x in range(1, num_records):  # for all record slots in RID page starting from 1 (skip 0 because that's the TPS)
                    rid_page_address.row = x
                    # read the rid number from page and convert from bytes to int
                    rid = int.from_bytes(lstore.globals.diskManager.read(self.table.name, rid_page_address), byteorder='big',signed=False)
                    if rid == 0:
                        continue
                    key = self.table.return_record(rid, 0, query_columns).columns[column_number]
                    rid_list = self.indexDict[column_number].get(key)
                    if rid_list != None:  # the key already is in index so just append the rid to the mapped list
                        rid_list = rid_list.append(rid)
                    else:  # the key doesn't exist in index yet
                        self.indexDict[column_number][key] = [rid]
        lstore.globals.icon.unlatch()

    """
    deletes record from index
    """
    def delete(self, key, RID, column_number):
        if self.indexDict[column_number].get(key):
            ridList = self.indexDict[column_number][key]
            if len(ridList) == 1:
                del self.indexDict[column_number][key]
            else:
                ridList.remove(RID)
                self.indexDict[column_number][key] = ridList
            return 1
        return 0

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indexDict[column_number].clear()

class PageDirectory:
    def __init__(self):
        self.indexDict = {}
        self.write_latch = Latch()
    """
    # add key-value pair mapping RID to page + offset
    :param value: {RID : [(base/tail, page_num), record_offset]}
    """

    def write(self, RID, value):
        self.write_latch.latch()
        self.indexDict[RID] = value
        self.write_latch.unlatch()

    """
    # find page + offset of given RID
    :return: {RID : [(base/tail, page_num), record_offset]}
    """

    def read(self, RID):
        self.write_latch.acquire()
        if self.indexDict.get(RID):
            self.write_latch.release()
            return self.indexDict[RID]
        self.write_latch.release()
        return 0

    """
    deletes record from index
    """
    def delete(self, RID, column_number=0):
        self.write_latch.latch()
        if self.indexDict.get(RID):
            del self.indexDict[RID]
            self.write_latch.unlatch()
            return 1
        self.write_latch.unlatch()
        return 0

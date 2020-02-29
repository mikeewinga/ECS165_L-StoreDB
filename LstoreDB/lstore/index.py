import lstore.table
import math
from lstore.config import *

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

class Index:

    def __init__(self, table):
        self.table = table
        self.diskManager = table.diskManager
        self.hasIndex = [0]*table.num_columns
        #self.indexDict = [{}]*table.num_columns
        self.indexDict = []
        for i in range(table.num_columns):
            self.indexDict.append({})  # each dictionary maps {key value : list of RID's}

    """
    # returns the location of all records with the given value
    # i.e. an RID of base page
    :param value: user-given key
    :return: list of RID's
    """
    def locate(self, column, value):
        #intList = [] # saves the RID's of matching records
        if self.indexDict[column].get(value):  # check if given key exists in indexDict
            ridList = self.indexDict[column][value]
            # convert byte format to RID numbers
            #for x in byteList:
                #intList.append(x)
            return ridList
        else:
            return []

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        pass

    def update(self, rid, original, *input):
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

    """
    # optional: Create index on specific column
    :param column_number: int
    """

    def create_index(self, column_number):
        if self.hasIndex[column_number]:
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
                num_records = self.diskManager.page_num_records(self.table.name, rid_page_address)
                for x in range(1, num_records):  # for all record slots in RID page starting from 1 (skip 0 because that's the TPS)
                    rid_page_address.row = x
                    # read the rid number from page and convert from bytes to int
                    rid = int.from_bytes(self.diskManager.read(self.table.name, rid_page_address), byteorder='big',signed=False)
                    key = self.table.return_record(rid, query_columns)[column_number]
                    rid_list = self.indexDict[column_number].get(key)
                    if rid_list != None:  # the key already is in index so just append the rid to the mapped list
                        rid_list = rid_list.append(rid)
                    else:  # the key doesn't exist in index yet
                        self.indexDict[column_number][key] = [rid]

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
    """
    # add key-value pair mapping RID to page + offset
    :param value: {RID : [(base/tail, page_num), record_offset]}
    """

    def write(self, RID, value):
        self.indexDict[RID] = value

    """
    # find page + offset of given RID
    :return: {RID : [(base/tail, page_num), record_offset]}
    """

    def read(self, RID):
        if self.indexDict.get(RID):
            return self.indexDict[RID]
        return 0

    """
    deletes record from index
    """
    def delete(self, RID, column_number=0):
        if self.indexDict.get(RID):
            del self.indexDict[RID]
            return 1
        return 0

class Address:
    #Base/Tail flag, Page-range number, Page number, Row number
    def __init__(self, pagerange, flag, pagenumber, row = None):
        self.pagerange = pagerange
        self.flag = flag  # values: 0--base, 1--tail, 2--base page copy used for merge
        self.pagenumber = pagenumber
        self.page = (flag, pagenumber)
        self.row = row

    def __add__(self, offset):
        #ret = (self.page[0],self.page[1]+offset)
        #return ret
        new_page_num = self.page[1] + offset
        return Address(self.pagerange, self.flag, new_page_num, self.row)

    def copy(self):
        return Address(self.pagerange, self.flag, self.pagenumber, self.row)

    def change_flag(self, flag):
        self.flag = flag
        self.page = (flag, self.pagenumber)

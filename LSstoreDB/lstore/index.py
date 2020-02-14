import lstore.table
import math
from lstore.config import *

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

class Index:

    """
    indexDict as index: {key : RID}
    indexDict as page directory: {RID : [(base/tail, page_num), record_offset]}
    -- base=0 tail=1
    """
    def __init__(self, table=None):
        if not table is None:
            self.table = table
        self.indexDict = {}
        pass

    """
    # returns the location of all records with the given value
    # i.e. an RID of base page
    :param value: user-given key
    :return: list of RID's
    """

    def locate(self, value):
        intList = [] # saves the RID's of matching records
        if self.indexDict.get(value):  # check if given key exists in indexDict
            byteList =  self.indexDict[value]
            # convert byte format to RID numbers
            for x in byteList:
                intList.append(int.from_bytes(x,byteorder='big',signed=False))
        return intList


    """
    # optional: Create index on specific column
    :param column_number: int
    """

    def create_index(self, table, column_number):
        self.table = table

        # number of pages needed for index
        numIndexPages = table.current_Rid_base // RANGESIZE

        # for every record, map the key of given column number to RID and save in dictionary 
        step = NUM_METADATA_COLUMNS + table.num_columns
        for i in range(0, numIndexPages+1):
            for j in range(0,table.pageranges[i].bOffSet+1,step):
                print(i, j)
                keyPage = table.pageranges[i].pages[(0, NUM_METADATA_COLUMNS+column_number+j)]
                ridPage = table.pageranges[i].pages[(0, 1+j)]
                for x in range(0, keyPage.num_records):
                    key = int.from_bytes(keyPage.read(x),byteorder='big',signed=False)
                    F = self.indexDict.get(key)
                    if F != None:
                        F = F.append(ridPage.read(x))
                    else:
                        self.indexDict[key] = [ridPage.read(x)]
        pass

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
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass


class Address:
    #Base/Tail flag, Page-range number, Page number, Row number
    def __init__(self, pagerange, flag, pagenumber, row):
        self.pagerange = pagerange
        self.page = (flag, pagenumber)
        self.row = row
        
    def __add__(self, offset):
        ret = (self.page[0],self.page[1]+offset)
        return ret

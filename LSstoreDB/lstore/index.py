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
        self.table = None
        if not table is None:
            self.table = table
            self.hasIndex = [0]*table.num_columns
            self.indexDict = [{}]*table.num_columns
        else:
            self.indexDict = {}
        pass

    """
    # returns the location of all records with the given value
    # i.e. an RID of base page
    :param value: user-given key
    :return: list of RID's
    """

    def locate(self, column, value):
        intList = [] # saves the RID's of matching records
        if self.indexDict[column].get(value):  # check if given key exists in indexDict
            byteList =  self.indexDict[column][value]
            # convert byte format to RID numbers
            for x in byteList:
                intList.append(x)
        return intList

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
        # number of pages needed for index
        numIndexPages = self.table.current_Rid_base // RANGESIZE
        # for every record, map the key of given column number to RID and save in dictionary 
        step = NUM_METADATA_COLUMNS + self.table.num_columns
        query_columns = [0] * self.table.num_columns
        query_columns[column_number] = 1
        for i in range(0, numIndexPages+1):
            for j in range(0,self.table.pageranges[i].bOffSet+1,step):
                keyPage = self.table.pageranges[i].pages[(0, NUM_METADATA_COLUMNS+j)]
                ridPage = self.table.pageranges[i].pages[(0, 1+j)]
                for x in range(0, ridPage.num_records):
                    key = self.table.return_record(int.from_bytes(ridPage.read(x),byteorder='big',signed=False), query_columns)[column_number]
                    F = self.indexDict[column_number].get(key)
                    if F != None:
                        F = F.append(ridPage.read(x))
                    else:
                        self.indexDict[column_number][key] = [ridPage.read(x)]
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
    deletes record from index
    """
    def delete(self, RID, column_number=0):
        if self.table:
            if self.indexDict[column_number].get(RID):
                del self.indexDict[column_number][RID]
                return 1
        else:
            if self.indexDict.get(RID):
                del self.indexDict[RID]
                return 1
        return 0

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
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

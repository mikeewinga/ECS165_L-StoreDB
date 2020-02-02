from table import Table
import math
from config import *

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.indexDict = {}
        pass

    """
    # returns the location of all records with the given value
    # i.e. an RID of base page
    """

    def locate(self, value):

        # listOfKeys = []

        # #returns an iterable sequence of all key value pairs
        # listOfItems = self.indexDict.items()
        
        # for item  in listOfItems:
        #     valueList = item[1]
        #     if value in valueList:
        #         listOfKeys.append(item[0])
        # return  listOfKeys
        # #pass

        intList = []
        byteList =  self.indexDict[value]
        for x in byteList:
            intList.append(int.from_bytes(x,byteorder='big',signed=False))
        return intList


    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        self.table = table

        # number of pages needed for index
        numIndexPages = math.ceil( self.table.current_Rid / (PAGESIZE/DATASIZE) )

        step = 4 + table.num_columns
        for i in range(0, numIndexPages):
            keyPage = table.page_directory[(0, 4+column_number+(i*step))]
            ridPage = table.page_directory[(0, 1+(i*step))]
            for x in range(0, keyPage.num_records):
                key = int.from_bytes(keyPage.read(x),byteorder='big',signed=False)
                #print(key)
                F = self.indexDict.get(key)
                if F != None:
                    F = F.append(ridPage.read(x))
                else:
                    self.indexDict[key] = [ridPage.read(x)]
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass

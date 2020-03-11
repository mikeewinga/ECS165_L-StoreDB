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

    def __str__(self):
        output = "(" + str(self.pagerange) + ", " + str(self.page) + ", " + str(self.row) + ")"
        return output

    def copy(self):
        return Address(self.pagerange, self.flag, self.pagenumber, self.row)

    def change_flag(self, flag):
        self.flag = flag
        self.page = (flag, self.pagenumber)

    def get_tuple(self):
        return (self.pagerange, self.page, self.row)

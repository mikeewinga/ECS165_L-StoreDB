

class Bufferpool:
    def __init__(self, max_pages):
        self.max_pages = max_pages
        self.page_map = {}  # {[string table_name, int page_num] : Page()}

    def is_in_pool(self, table_name, page_num):
        

"""
-bufferpool with max page size
    -dictionary that maps table name + abstract page number to corresponding page in pool
    -note that there could be pages from multiple tables files in the one bufferpool
-disk_manager class:
    -instance of bufferpool object
    -dictionary file directory that maps table names to the file on disk
    -function that copies the needed page from file into bufferpool
    -function that evicts page from bufferpool and flushes it to file if dirty
    -function that takes in table name and page number, and maps to either:
        -the page in bufferpool
        -the physical page on file (and brings it into memory)
    -functions to read/write the pages in bufferpool
-each table has corresponding file, and the file stores pages in sets of multiple page ranges
    -10 page ranges in 10,000 record table
    -4 KB per loadable chunk of memory
    -format it so that seeking is easy
    -will want to convert into bytearray
-file manipulation:
    -opening file just gets you file pointer
    -any operation on file will load file into memory, but if you only want to partially load the
    file, then use process(line, file seek(), chunk = infile.read(chunksize) etc. for partial loading into RAM
        -https://stackoverflow.com/questions/6475328/how-can-i-read-large-text-files-in-python-line-by-line-without-loading-it-into
"""

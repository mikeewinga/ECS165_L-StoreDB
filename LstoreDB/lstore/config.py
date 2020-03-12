# Global Setting for the Database
# PageSize, StartRID, etc..
PAGESIZE=4096
DATASIZE=8
RANGESIZE=1022
BUFFERPOOL_SIZE=1000

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN=4
NUM_METADATA_COLUMNS=5

COLUMN_BLOCK_PAGES = 10

# tuple indexing for table metadata (primary_key, num_columns)
PRIMARY_KEY = 0
COLUMNS = 1
BASE_RID = 2
TAIL_RID = 3
PRID = 4
# PRANGE_METADATA is a tuple that contains BOFFSET and TOFFSET
PRANGE_METADATA = 5
BOFFSET = 0
TOFFSET = 1
CUR_TID = 2
MOFFSET = 3
MERGE_F = 4

# tuple indexing for table page metadata (file_offset, num_records)
FILE_OFFSET = 0
NUM_RECORDS = 1

BIN_EXTENSION = ".bin"
INDEX_EXTENSION = "_index.txt"
PAGE_DIR_EXTENSION = "_pageDir.txt"
COLUMN_BLOCK_BYTES = PAGESIZE * COLUMN_BLOCK_PAGES

#lock types
IS = 1
IX = 2
S  = 3 
X  = 4

#lock levels
DATABASE    = 0
TABLE       = 1
PAGERANGE   = 2 
PAGE        = 3
ROW         = 4

#lockManager switch statement opp
INSERT  = 1
SELECT  = 2
SUM     = 2
UPDATE  = 1
DELETE  = 1
#Sum and select have the same opp

def init():
    pass

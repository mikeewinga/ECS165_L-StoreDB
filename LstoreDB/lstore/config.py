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

# tuple indexing for table page metadata (file_offset, num_records)
FILE_OFFSET = 0
NUM_RECORDS = 1

BIN_EXTENSION = ".bin"
INDEX_EXTENSION = "_index.txt"
PAGE_DIR_EXTENSION = "_pageDir.txt"
COLUMN_BLOCK_BYTES = PAGESIZE * COLUMN_BLOCK_PAGES

def init():
    pass

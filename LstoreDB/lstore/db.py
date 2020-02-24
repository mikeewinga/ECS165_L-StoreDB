from lstore.table import Table
from lstore.disk_manager import DiskManager

class Database():

    def __init__(self):
        #self.tables = {}  # maps {string name : Table}
        #self.num_tables = 0
        self.diskManager = DiskManager()
        pass

    def open(self, path):
        self.diskManager.set_directory_path(path)
        pass

    def close(self):
        self.diskManager.close()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        if (self.diskManager.new_table_file(name, key, num_columns)):  # check if new table file was successfully created
            table = Table(name, key, num_columns, self.diskManager)
            return table
        else:
            return None
        #self.tables[name] = table
        #self.num_tables = self.num_tables + 1

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        #FIXME
        if name in self.tables:
            del self.tables[name]
            self.num_tables = self.num_tables - 1
        pass

    """
    # Retruns table with the passed name
    """
    def get_table(self, name):
        table_metadata = self.diskManager.open_table_file(name)
        if (len(table_metadata) != 0):  # the table file and metadata exist
            table = Table(name, table_metadata[PRIMARY_KEY], table_metadata[COLUMNS], self.diskManager)
            return table
        else:
            return None

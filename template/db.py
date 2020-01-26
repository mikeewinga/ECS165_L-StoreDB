from table import Table

class Database():

    def __init__(self):
        self.tables = {}
        pass

    def open(self):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, key, num_columns):
        table = Table(name, num_columns, key)
        self.tables[name] = table
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables.pop(name)
        pass

from lstore.db import Database
from lstore.query import Query
from lstore.config import init

from random import choice, randint, sample, seed
from colorama import Fore, Back, Style

# Student Id and 4 grades
init()
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

record1 = [1, 10, 20, 30, 40]
record2 = [2, 10, 22, 33, 44]
record3 = [2, 11, 22, 55, 66]

query.insert(*record1)
query.insert(*record2)
query.insert(*record3)

print("original data:")
print(record1)
print(record2)
print(record3)
print("\n")

key = 2
print("demo duplicate key selection, where key = " + str(key) + "\n")
record = query.select(key, [1, 1, 1, 1, 1])
for r in record:
    print(r)

print("\n")
input()

updated_columns = [None, 100, 200, 300, None]
key = 2
print("demo multiple column update, where key = " + str(key))
print("new column data to update: ")
print(updated_columns)
print("\n")
query.update(key, *updated_columns)
print("updated records:")
record = query.select(key, [1, 1, 1, 1, 1])
for r in record:
    print(r)

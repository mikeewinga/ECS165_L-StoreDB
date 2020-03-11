from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

from random import choice, randint, sample, seed

# Student Id and 4 grades
db = Database()
db.open('~/ECS165')
grades_table = db.get_table('Grades')
query = Query(grades_table)

t = Transaction()

# repopulate with random data
records = {}
seed(3562901)
for i in range(0, 2000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
keys = sorted(list(records.keys()))
for _ in range(2):
    for key in keys:
        for j in range(1, grades_table.num_columns):
            value = randint(0, 20)
            records[key][j] = value
keys = sorted(list(records.keys()))
for key in keys:
    print(records[key])

for key in keys:
    #record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    t.add_query(query.select, key, 0, [1, 1, 1, 1, 1])

deleted_keys = sample(keys, 100)
for key in deleted_keys:
    #query.delete(key)
    t.add_query(query.delete, key)
    records.pop(key, None)

with open("aggregate2.txt", "x") as file:
    for i in range(0, 100):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
        file.write(str(column_sum) + "\n")
        t.add_query(query.sum, keys[r[0]], keys[r[1]], 0)
        #result = query.sum(keys[r[0]], keys[r[1]], 0)

t.run()

db.close()

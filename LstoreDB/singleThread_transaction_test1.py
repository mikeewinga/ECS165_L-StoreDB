from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

t = Transaction()

records = {}
seed(3562901)
for i in range(0, 2000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    #t.add_query(query.insert, *records[key]) #FIXME uncomment later


keys = sorted(list(records.keys()))
for key in keys:
    #record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    t.add_query(query.select, key, 0, [1, 1, 1, 1, 1])

for _ in range(2):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            #query.update(key, *updated_columns)
            t.add_query(query.update, key, *updated_columns)
            #record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            t.add_query(query.select, key, 0, [1, 1, 1, 1, 1])

"""
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for j, column in enumerate(record.columns):
        if column != records[key][j]:
            error = True
    if error:
        print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
print("Self Update Finished(delete this later)")
"""

with open("aggregate.txt", "x") as file:
    for i in range(0, 100):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
        file.write(str(column_sum) + "\n")
        t.add_query(query.sum, keys[r[0]], keys[r[1]], 0)
        #result = query.sum(keys[r[0]], keys[r[1]], 0)

t.run()

db.close()

exit()
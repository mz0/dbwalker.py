import mysql.connector
import time

dbh = '127.0.0.1'
db1 = 'shsha_Blue'
db2 = 'shsha_Green'
dbu = 'shsha'
dbp = '999'

"""counting `data_warehouse` is pointless and takes too long"""
count_approx = '''
SELECT table_name
FROM information_schema.tables
WHERE
table_schema = %s
and table_name <> 'data_warehouse'
and table_name <> 'flyway_schema_history'
and NOT table_name like 'SHSHA_%'
'''


class CountComparator:
    def __init__(self):
        self.count1 = -1
        self.count2 = -1
        self.time1 = -0.1
        self.time2 = -0.1

    def has_diffs(self):
        return False if self.count1 == self.count2 else True

    explain = 'count1, time1, count2, time2 (if > 0.001)'
    def __str__(self):
        # d1 = self.count1 - self.count2
        time2 = f'{self.time2:.3f}' if self.time2 > 0.001 else '0'
        return f'{self.count1},{self.count2},{time2}'


comparison = {}
cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)
cursor = cnx.cursor()
cursor.execute(count_approx, (db1,))
for (table,) in cursor:
    c = CountComparator()
    comparison[table] = c

print(f'DB1 {db1}: {len(comparison)} tables')
count = 'SELECT COUNT(*) FROM '
for table in comparison.keys():
    start = time.time()
    cursor.execute(count+'`'+db1+'`.`'+table+'`')
    end = time.time()
    if cursor.lastrowid != 0: print("last rowId: ", cursor.lastrowid)
    cnt = 0
    (cnt,) = cursor.fetchone()
    comparison[table].count1 = cnt
    comparison[table].time1 = end - start

db2_tables = []
cursor.execute(count_approx, (db2,))
for (table,) in cursor:
    db2_tables.append(table)
    c = comparison[table] if table in comparison else CountComparator()
    comparison[table] = c

for table in db2_tables:
    start = time.time()
    cursor.execute(count+'`'+db2+'`.`'+table+'`')
    end = time.time()
    if cursor.lastrowid != 0: print("last rowId: ", cursor.lastrowid)
    cnt = 0
    (cnt,) = cursor.fetchone()
    comparison[table].count2 = cnt
    comparison[table].time2 = end - start


print(f'DB2 {db2}: {len(db2_tables)} tables')
diff_found = False
print(f'Table name, {CountComparator.explain}')
for (k, v) in comparison.items():
    if v.has_diffs():
        diff_found = True
        print(f'{k},{v}')

if not diff_found:
    print("No diffs found")

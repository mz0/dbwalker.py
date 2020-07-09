import mysql.connector
import time

dbh = '127.0.0.1'
db1 = 'shsha'
dbu = 'shsha'
dbp = '999'

count_approx = '''
SELECT table_name, table_rows
FROM information_schema.tables
WHERE
table_schema = %s
and
data_length > 0
and table_name <> 'flyway_schema_history'
and NOT table_name like 'SHSHA_%'
'''


class CountComparator:
    def __init__(self):
        self.approx1 = -1
        self.approx2 = -1
        self.count1 = -1
        self.count2 = -1
        self.counted1 = -0.1
        self.counted2 = -0.1

    def has_diffs(self):
        return False if self.approx1 == self.approx2 else True

    explain = 'approx1,approx2'
    def __str__(self):
        da = self.approx2 - self.count1
        d1 = self.count1 - self.approx1
        return f'{self.approx1} {self.count1} {self.approx2}'


comparison = {}
cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)
cursor = cnx.cursor()
cursor.execute(count_approx, (db1,))
for (table, rows) in cursor:
    c = CountComparator()
    c.approx1 = rows
    comparison[table] = c

print(f'DB1 {db1}: {len(comparison)} non-empty tables')
count = 'SELECT COUNT(*) FROM '
for table in comparison.keys():
    start = time.time()
    cursor.execute(count+'`'+db1+'`.`'+table+'`')
    end = time.time()
    if cursor.lastrowid != 0: print("last rowId: ", cursor.lastrowid)
    cnt = 0;
    (cnt,) = cursor.fetchone()
    comparison[table].count1 = cnt
    comparison[table].counted1 = end - start
    # print(f'{table} {cnt} rows, counted in {end-start:.2f}s')

db2 = 'shsha_Blue'
db2_tables = 0
cursor.execute(count_approx, (db2,))
for (table, rows) in cursor:
    db2_tables += 1
    c = comparison[table] if table in comparison else CountComparator()
    c.approx2 = rows
    comparison[table] = c

print(f'DB2 {db2}: {db2_tables} non-empty tables')
diff_found = False
print(f'Table name: {CountComparator.explain}')
for (k, v) in comparison.items():
    if v.has_diffs():
        diff_found = True
        print(f'{k}  {v}')

if not diff_found:
    print("No diffs found")

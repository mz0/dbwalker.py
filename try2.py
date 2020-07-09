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
table_schema = DATABASE()
and
data_length > 0
and table_name <> 'flyway_schema_history'
and NOT table_name like 'SHSHA_%'
'''

tabs1 = []
cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)
cursor = cnx.cursor()
cursor.execute(count_approx)
for (table, rows) in cursor:
    tabs1.append(table)

count = 'SELECT COUNT(*) FROM '
for table in tabs1:
    start = time.time()
    cursor.execute(count+'`'+db1+'`.`'+table+'`')
    end = time.time()
    if cursor.lastrowid != 0: print("last rowId: ", cursor.lastrowid)
    cnt = 0;
    (cnt,) = cursor.fetchone()
    print(f'{table} {cnt} rows, counted in {end-start:04.2f}s')

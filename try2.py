import mysql.connector

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

print(tabs1)

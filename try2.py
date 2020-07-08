import mysql.connector

dbh = '127.0.0.1'
db1 = 'shsha_Green'
dbu = 'shsha'
dbp = '999'

count_approx='''
SELECT table_name, table_rows
FROM information_schema.tables
WHERE
table_schema = DATABASE()
and
data_length > 0
'''

cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)
cursor = cnx.cursor()
cursor.execute(count_approx)
for (table, rows) in cursor:
    print(table)

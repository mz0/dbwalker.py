#!/usr/bin/python3

# pip3 install --user mysql-connector-python
import mysql.connector

dbh='127.0.0.1' # 10.44.68.150
db1='shsha'
dbu='shshaG'
dbp='999'

cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)

cursor = cnx.cursor()

query = ("SELECT COLUMN_NAME, COLUMN_TYPE"
" FROM INFORMATION_SCHEMA.COLUMNS"
" WHERE TABLE_NAME = %s AND TABLE_SCHEMA=%s"
" ORDER BY ORDINAL_POSITION"
)

cursor.execute(query, ('data_warehouse_conformance', db1))

for (fld1, fld2) in cursor:
  print(f'{fld1}  {fld2}')

cursor.close()
cnx.close()
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html

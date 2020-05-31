#!/usr/bin/python3

# pip3 install --user mysql-connector-python
import mysql.connector

dbh='127.0.0.1'
db1='oxanasm6_navigat'
dbu='shshaG'
dbp='GSp974ax'

cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)

cursor = cnx.cursor()

query = ("SELECT first_name, last_name, hire_date FROM employees "
         "WHERE hire_date BETWEEN %s AND %s")

hire_start = datetime.date(1999, 1, 1)
hire_end = datetime.date(1999, 12, 31)

cursor.execute(query, (hire_start, hire_end))

for (first_name, last_name, hire_date) in cursor:
  print("{}, {} was hired on {:%d %b %Y}".format(
    last_name, first_name, hire_date))

cursor.close()
cnx.close()
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html

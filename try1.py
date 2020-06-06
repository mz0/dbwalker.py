#!/usr/bin/python3

# pip3 install --user mysql-connector-python
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html
import mysql.connector

dbh = '127.0.0.1'
db1 = 'shsha_Blue'
dbu = 'shsha'
dbp = '999'

cnx = mysql.connector.connect(user=dbu, password=dbp, host=dbh, database=db1)

cursor = cnx.cursor()

desc = '''
SELECT COLUMN_NAME, COLUMN_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
ORDER BY ORDINAL_POSITION
'''

tbl_conform_results = 'data_warehouse_conformance'

# cursor.execute(desc, (tbl_conform_results, db1))
# for (fld1, fld2) in cursor: print(f'{fld1}  {fld2}')

conform_result_count = '''
SELECT count(id) cnt, conformance, status, uid
FROM `data_warehouse_conformance`
group by conformance, status, uid
order by conformance, uid
'''

cursor.execute(conform_result_count)
conform_rule_str = []
conform_results = {}
captures = {}

for (count, rstr, passed, captid) in cursor:
    if conform_rule_str.count(rstr) < 1: conform_rule_str.append(rstr)  # make an index of Conformance "names"
    crn = conform_rule_str.index(rstr)  # current Conformance index
    tf = True if passed == 'PASSED' else False
    if not crn in conform_results:
        conform_results[crn]= {}
        conform_results[crn][True] = []
        conform_results[crn][False] = []
    conform_results[crn][tf].append((captid, count))

cursor.close()
cnx.close()

print(' Pass* Fail* Conform Rule (* count of capture files)')
for i in range(len(conform_rule_str)):
    cr = conform_results[i]
    print(f'{str(len(cr[True])).rjust(5," ")} {str(len(cr[False])).rjust(5," ")}  {conform_rule_str[i]}')

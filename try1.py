#!/usr/bin/python3

# pip3 install --user mysql-connector-python
import mysql.connector

dbh = '127.0.0.1'  # 10.44.68.150
db1 = 'shsha'
dbu = 'shshaG'
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

cursor.execute(desc, (tbl_conform_results, db1))

for (fld1, fld2) in cursor:
    print(f'{fld1}  {fld2}')

conform_result_count = '''
SELECT count(id) cnt, conformance, status, uid
FROM `data_warehouse_conformance`
group by conformance, status, uid
order by conformance, uid
'''

cursor.execute(conform_result_count)
conform_rule_str = []
conform_passed = {}
conform_other = {}
captures = {}

for (count, rstr, passed, captid) in cursor:
    if conform_rule_str.count(rstr) < 1: conform_rule_str.append(rstr)  # make index or Conformance "names"
    crn = conform_rule_str.index(rstr)  # current Conformance index

    # make 2 dicts for PASSED and others
    if passed == 'PASSED':
        if crn in conform_passed:
            conform_passed[crn].append((captid, count))
        else:
            conform_passed[crn] = [(captid, count)]
    else:
        if crn in conform_other:
            conform_other[crn].append((captid, count))
        else:
            conform_other[crn] = [(captid, count)]

    # for each "capture_id" save PASSED/FAILED counts
    if captid in captures:
        captures[captid].append((crn, passed, count))
    else:
        captures[captid] = [(crn, passed, count)]

print(f'Conform Rules Passed Number {len(conform_passed)}')
print(f'Conform Rules other (e.g. Failed) Number {len(conform_other)}')
print(f'CaptureIDs with results: {len(captures)}')

print(f'Conform Rules: {conform_rule_str}')
cursor.close()
cnx.close()
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html

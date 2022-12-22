from HiveConnector import *
import time

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

while True:
    hiveSelect(hive, ['airline', 'dest'], 'druid_flights', limit=10)
    hiveSelect(hive, ['tail_number', 'taxi_out', 'taxi_in', 'airline'], 'druid_flights', where_column='airline', where_sign='=', where='B6', limit=500)
    hiveShowTablesList(hive)
    hiveSelect(hive, ['tail_number', 'origin', 'dest', 'arr_time'], 'druid_flights', where_column='arr_time', where_sign='<', where='15', limit=100)
    time.sleep(30)
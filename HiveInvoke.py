from HiveConnector import *
import time

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

start = time.time()
e1 = hiveExecuteSelect(hive, 'show tables')
end = time.time()
print(round(end - start, 4), "s\n")

start = time.time()
e2 = hiveSelect(hive, ['dest_state', 'dest_state_fips', 'dest_state_name'], 'druid_states')
end = time.time()
print(round(end - start, 4), "s\n")

start = time.time()
e3 = hiveExecuteSelect(hive, '''
    SELECT
        actual_elapsed_time,
        air_time,
        airline,
        arr_del_15,
        arr_delay,
        arr_delay_minutes,
        arr_time
    FROM druid_filghts
    LIMIT 50
''')
print(round(end - start, 4), "s\n")
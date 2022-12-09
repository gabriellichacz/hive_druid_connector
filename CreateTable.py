from HiveConnector import *
import time

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

########### Creating external tables in Hive from data warehouse tables in Druid ###########

hiveCreateDruidExternalTable(hive, 'druid_airlines', 'airlines')
hiveCreateDruidExternalTable(hive, 'druid_airfields', 'airfields')
hiveCreateDruidExternalTable(hive, 'druid_states', 'states')
hiveCreateDruidExternalTable(hive, 'druid_flights', 'old_data')
from HiveConnector import *
import time

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

########### Making sure base data warehouse tables in Hive are of correct size ###########

airlinesLen = countData(hive, 'druid_airlines', 'code')
airfieldsLen = countData(hive, 'druid_airfields', 'airport_id')
statesLen = countData(hive, 'druid_states', 'dest_state')
flightsLen = countData(hive, 'druid_flights', 'dest')

print('airlines length:', airlinesLen)
print('airfields length:', airfieldsLen)
print('states length:', statesLen)
print('flights length:', flightsLen)
from DruidConnector import *
from HiveConnector import *
import time

##################################################################
# Creating table in Druid
# Creating external table in Hive from Druid table
# Inserting external table data to main data warehouse table in Hive
# Meanwhile measuring execution time
##################################################################

druidUrl = druidConnect("192.168.1.112", "8082", "sql")
druidStatusUrl = druidConnect("192.168.1.112", "8081", "task")

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

druidTableName = "test_data"
csvDataName = "data_5tys"
start = time.time()

taskId = druidCreateTableFromCsv(druidUrl, druidTableName, "/home/stud/Downloads/import_data", csvDataName, [
    ['flight_id', 'long'],
    ['flight_date', 'string'],
    ['airline','string'],
    ['origin','string'],
    ['dest','string'],
    ['cancelled','string'],
    ['diverted','string'],
    ['crs_dep_time','long'],
    ['dep_time','long'],
    ['dep_delay_minutes','long'],
    ['dep_delay','long'],
    ['arr_time','long'],
    ['arr_delay_minutes','long'],
    ['air_time','long'],
    ['crs_elapsed_time','long'],
    ['actual_elapsed_time','long'],
    ['distance','long'],
    ['day_of_week','long'],
    ['flight_number_marketing_airline','long'],
    ['tail_number','string'],
    ['flight_number_operating_airline','long'],
    ['origin_airport_id','long'],
    ['dest_airport_id','long'],
    ['dep_del_15','long'],
    ['departure_delay_groups','long'],
    ['dep_time_blk','string'],
    ['taxi_out','long'],
    ['wheels_off','long'],
    ['wheels_on','long'],
    ['taxi_in','long'],
    ['crs_arr_time','long'],
    ['arr_delay','long'],
    ['arr_del_15','long'],
    ['arrival_delay_groups','long'],
    ['arr_time_blk','string'],
    ['distance_group','long'],
    ['div_airport_landings','long']
], 1, 'day')

status = "RUNNING"
while status != "SUCCESS":
    try:
        status = druidCheckTaskStatus(druidStatusUrl, taskId)['multiStageQuery']['payload']['status']['status']
    except:
        status = "RUNNING"
        
hiveExternalTableName = "temp_druid_" + druidTableName
hiveCreateDruidExternalTable(hive, hiveExternalTableName, druidTableName)

insertDataFromTableToTable(hive, hiveExternalTableName, flights, [
    'flight_id',
    '`__time` as flight_date',
    'airline',
    'origin',
    'dest',
    'cancelled',
    'diverted',
    'crs_dep_time',
    'dep_time',
    'dep_delay_minutes',
    'dep_delay',
    'arr_time',
    'arr_delay_minutes',
    'air_time',
    'crs_elapsed_time',
    'actual_elapsed_time',
    'distance',
    'day_of_week',
    'flight_number_marketing_airline',
    'tail_number',
    'flight_number_operating_airline',
    'origin_airport_id',
    'dest_airport_id',
    'dep_del_15',
    'departure_delay_groups',
    'dep_time_blk',
    'taxi_out',
    'wheels_off',
    'wheels_on',
    'taxi_in',
    'crs_arr_time',
    'arr_delay',
    'arr_del_15',
    'arrival_delay_groups',
    'arr_time_blk',
    'distance_group',
    'div_airport_landings'
])
end = time.time()

hiveDropTable(hive, hiveExternalTableName)

print("Whole operation took:", end-start)
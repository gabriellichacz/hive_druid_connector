from HiveConnector import *
import time

hive_host = '192.168.1.112'
hive_port = '10000'
hive_username = 'stud'
hive_pass = 'stud'
hive = hiveConnect(hive_host, hive_port, hive_username, hive_pass)

########### Creating base data warehouse tables in Hive ###########

hiveCreateTable(hive, 'states', [
    'dest_state string',
    'dest_state_fips int',
    'dest_state_name string'
])

hiveInsertDataToTableFromCsv(hive, 'states', '/home/stud/Downloads/old_data/states')

hiveCreateTable(hive, 'airfields', [
    'airport_id int',
    'airport_seq_id int',
    'origin_city_market_id int',
    'origin_city_name string'
])

hiveInsertDataToTableFromCsv(hive, 'airfields', '/home/stud/Downloads/old_data/airfields')

hiveCreateTable(hive, 'airlines', [
    'code string',
    'description string'
])

hiveInsertDataToTableFromCsv(hive, 'airlines', '/home/stud/Downloads/old_data/airlines')

hiveCreateTable(hive, 'flights', [
    'flight_id int',
    'flight_date string',
    'airline string',
    'origin string',
    'dest string',
    'cancelled string',
    'diverted string',
    'crs_dep_time int',
    'dep_time int',
    'dep_delay_minutes int',
    'dep_delay int',
    'arr_time int',
    'arr_delay_minutes int',
    'air_time int',
    'crs_elapsed_time int',
    'actual_elapsed_time int',
    'distance int',
    'day_of_week int',
    'flight_number_marketing_airline int',
    'tail_number string',
    'flight_number_operating_airline int',
    'origin_airport_id int',
    'dest_airport_id int',
    'dep_del_15 int',
    'departure_delay_groups int',
    'dep_time_blk string',
    'taxi_out int',
    'wheels_off int',
    'wheels_on int',
    'taxi_in int',
    'crs_arr_time int',
    'arr_delay int',
    'arr_del_15 int',
    'arrival_delay_groups int',
    'arr_time_blk string',
    'distance_group int',
    'div_airport_landings int'
])

hiveInsertDataToTableFromCsv(hive, 'flights', '/home/stud/Downloads/old_data/main_data_flights.csv')
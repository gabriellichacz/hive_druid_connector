from DruidConnector import *
import time

druidUrl = druidConnect("192.168.1.112", "8082")

test = druidCreateTableFromCsv(druidUrl, 'test_data', '/home/stud/Downloads/import_data', 'data_5tys', [
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
print(test)
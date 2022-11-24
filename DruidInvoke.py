from DruidConnector import *
import time

druidUrl = druidConnect("192.168.1.112", "8082")

insertData = druidInsert(druidUrl, 'wikipedia', 'timestamp', 'https://druid.apache.org/data/wikipedia.json.gz', 'json', [
        ['added', 'long'],
        ['channel', 'string'],
        ['cityName', 'string'],
        ['comment', 'string'],
        ['commentLength', 'long'],
        ['countryIsoCode', 'string'],
        ['countryName', 'string'],
        ['deleted', 'long'],
        ['delta', 'long'],
        ['deltaBucket', 'string'],
        ['diffUrl', 'string'],
        ['flags', 'string'],
        ['isAnonymous', 'string'],
        ['isMinor', 'string'],
        ['isNew', 'string'],
        ['isRobot', 'string'],
        ['isUnpatrolled', 'string'],
        ['metroCode', 'string'],
        ['namespace', 'string'],
        ['page', 'string'],
        ['regionIsoCode', 'string'],
        ['regionName', 'string'],
        ['timestamp', 'string'],
        ['user', 'string']
    ]
)

print(insertData)
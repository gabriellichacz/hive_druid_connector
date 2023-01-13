from pssh.clients import SSHClient
from DruidConnector import *
import time

# Prerequisites
druidSqlUrl = druidConnect("192.168.1.112", "8082", "sql")
kafkaIp = '192.168.1.112'
csvName1 = 'data_5tys.csv'
csvName3 = 'data_500tys.csv'
csvName4 = 'data_1mln.csv'
csvName5 = 'data_2mln.csv'
csvName6 = 'data_4mln.csv'
csvName7 = 'data_5mln.csv'

def performanceTest(druidSqlUrl, kafkaHostIp, csvName) -> 'string':
    """
    Performance test function - data ingestion

    Parameters:
    druidSqlUrl (string): Druid database host URL
    kafkaHostIp (string): Server IP where the kafka is running
    csvName (string): csv file name to insert
  
    Returns:
    end-start (string): how long ingestion operation took
    """
    if csvName == 'data_5tys.csv':
        toAddRowsNumber = 5000
    elif csvName == 'data_500tys.csv':
        toAddRowsNumber = 500000
    elif csvName == 'data_1mln.csv':
        toAddRowsNumber = 1000000
    elif csvName == 'data_2mln.csv':
        toAddRowsNumber = 2000000
    elif csvName == 'data_4mln.csv':
        toAddRowsNumber = 4000000
    elif csvName == 'data_5mln.csv':
        toAddRowsNumber = 5000000
    else: 
        return "Wrong file name provided"
    
    countBeforeOperation = int(druidCountTable(druidSqlUrl, 'old_data')[13:-3])
    countAfterOperation = countBeforeOperation + toAddRowsNumber
    
    cmd = '''cd /opt/kafka_2.13-2.7.0 && ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic druid_stream < /home/stud/Downloads/import_data/''' + csvName
    client = SSHClient(kafkaHostIp, 'root', 'osource')
    
    # Operation start
    start = time.time()
    host_out = client.run_command(cmd) # Data ingestion into Kafka
    
    count = int(druidCountTable(druidSqlUrl, 'old_data')[13:-3])
    while count < countAfterOperation:
        count = int(druidCountTable(druidSqlUrl, 'old_data')[13:-3]) 
    end = time.time()
    # Operation end
    
    return end-start

fiveThousand = []
for i in range(9):
    fiveThousand.append(performanceTest(druidSqlUrl, kafkaIp, csvName1))
print("List of times for 5 thousand data rows:\n", fiveThousand)

fiveHundredThousand = []
for i in range(9):
    fiveHundredThousand.append(performanceTest(druidSqlUrl, kafkaIp, csvName3))
print("List of times for 500 thousands data rows:\n", fiveHundredThousand)

oneMillion = []
for i in range(9):
    oneMillion.append(performanceTest(druidSqlUrl, kafkaIp, csvName4))
print("List of times for 1 million data rows:\n", oneMillion)

twoMillion = []
for i in range(9):
    twoMillion.append(performanceTest(druidSqlUrl, kafkaIp, csvName5))
print("List of times for 2 million data rows:\n", twoMillion)

fourMillion = []
for i in range(9):
    fourMillion.append(performanceTest(druidSqlUrl, kafkaIp, csvName6))
print("List of times for 4 million data rows:\n", fourMillion)

fiveMillion = []
for i in range(1):
    fiveMillion.append(performanceTest(druidSqlUrl, kafkaIp, csvName7))
print("List of times for 5 million thousands data rows:\n", fiveMillion)
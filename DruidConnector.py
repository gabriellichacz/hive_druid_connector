# python 3.8.10 64-bit
try:
  import json
  import requests
except:
  cur = 'Couldn\'t import json or requests library'

def druidConnect(param_host, param_port = "8082", option = 'default') -> 'string':
  """
  Connect to Druid database

	Parameters:
  param_host (string): Druid database host
  param_port (string): Druid broker port | default = "8082"
  option (string): Type of url to return (sql, task, default)

	Returns:
  url (string): Druid connection url
  """
  url = "http://" + param_host + ":" + param_port
  
  if(option == 'sql'):
    url += "/druid/v2/sql/"
  elif(option == 'sql_task'):
    url += "/druid/v2/sql/task/"
  elif(option == 'task'):
    url += "/druid/indexer/v1/task/"
  else:
    url
      
  return url

def druidInsertIntoFromUrl(druidUrl, table_name, timestampColName, dataURL, dataFormat, columnList, partition = 'day') -> 'string':
  """
  Create 'insert into' task

  Parameters:
  druidUrl (string): Druid database host URL
  table_name (string): Druid table to insert data to
  timestampColName (string): Data timestamp column
  dataURL (string): Data url location
  dataFormat (string): Data format
  columnList (array): Data columns and their types eg. [colName, colType]
  partition (string): Data partition | default = day

  Returns:
  responseData['taskId'] (string): Druid task id
  """
  sqlQuery = '''INSERT INTO ''' + table_name + '''\n''' 
  sqlQuery += '''SELECT TIME_PARSE(\"''' + timestampColName + '''\") AS __time,\n*\n'''
  sqlQuery += '''FROM TABLE(\nEXTERN(\n'''
  sqlQuery += ''''{\"type\": \"http\", \"uris\": [\"''' + dataURL + '''\"]}',\n'''
  sqlQuery += ''''{\"type\": \"''' + dataFormat + '''\"'''
  sqlQuery += ',"findColumnsFromHeader":true'
  sqlQuery += '''}',\n'['''

  for column_index, column in enumerate(columnList):
      sqlQuery += '''{\"name\": \"''' + column[0] + '''\",\"type\": \"''' + column[1] + '''\"}'''
      sqlQuery += ',' if (column_index < len(columnList)-1) else ''

  sqlQuery += ''']'\n)\n)\n'''
  sqlQuery += '''PARTITIONED BY ''' + partition
  
  payload = json.dumps({
      "query": sqlQuery,
      "context": {
          "maxNumTasks": 3
      }
  })
  headers = {'Content-Type': 'application/json'}
  response = requests.request("POST", druidUrl, headers=headers, data=payload)
  responseData = response.json()
  print(responseData)
    
  return responseData['taskId']

def druidCreateTableFromCsv(druidUrl, table_name, localPathFolder, csvName, columnList, timestampColumnIndex, partition = 'day'):
  """
  Create 'create table' task

  Parameters:
  druidUrl (string): Druid database host URL
  table_name (string): New Druid table name
  localPathFolder (string): Local path to folder containing csv file
  csvName (string): Name of the csv file
  columnList (array): Data columns and their types eg. [colName, colType]
  timestampColumnIndex (string): columnList index of timestamp column
  partition (string): Data partition | default = day

  Returns:
  responseData['taskId'] (string): Druid task id
  """
  sqlQuery = '''REPLACE INTO ''' + table_name + ''' OVERWRITE ALL\n'''
  sqlQuery += '''WITH ext AS (SELECT *\n'''
  sqlQuery += '''FROM TABLE(\n'''
  sqlQuery += ''' EXTERN(\n'''
  sqlQuery += '''  '{"type":"local","baseDir":"'''+ localPathFolder + '''","filter":"'''+ csvName+ '''.csv"}',\n'''
  sqlQuery += '''  '{"type":"csv","findColumnsFromHeader":true}',\n'''
  sqlQuery += '''  '['''
  
  for column_index, column in enumerate(columnList):
    sqlQuery += '''{\"name\": \"''' + column[0] + '''\",\"type\": \"''' + column[1] + '''\"}'''
    sqlQuery += ',' if (column_index < len(columnList)-1) else ''
      
  sqlQuery += ''']'\n'''
  sqlQuery += ''' )\n))\n'''
  sqlQuery += '''SELECT\n'''
  
  sqlQuery += '''TIME_PARSE(''' + columnList[timestampColumnIndex][0] + ''') AS __time,\n'''
  for column_index, column in enumerate(columnList):
    sqlQuery += column[0] if (column_index != timestampColumnIndex) else ''
    sqlQuery += ',\n' if (column_index < len(columnList)-1 and column_index != timestampColumnIndex) else '\n'
    
  sqlQuery += '''FROM ext\n'''
  sqlQuery += '''PARTITIONED BY ''' + partition
  
  payload = json.dumps({
      "query": sqlQuery,
      "context": {
          "maxNumTasks": 3
      }
  })
  headers = {'Content-Type': 'application/json'}
  response = requests.request("POST", druidUrl, headers=headers, data=payload)
  responseData = response.json()
  print(responseData)
    
  return responseData['taskId']

def druidCheckTaskStatus(druidUrl, taskId) -> 'string':
  """
  Return task status

  Parameters:
  druidUrl (string): Druid database host URL
  taskId (string): task id

  Returns:
  response.json() (string): json string with task status
  """
  taskStatusUrl = druidUrl + taskId + "/reports"
  response = requests.get(taskStatusUrl)
  
  return response.json()

def druidCountTable(druidUrl, table_name) -> 'string':
  """
  Counts table rows

  Parameters:
  druidUrl (string): Druid database host URL
  table_name (string): table name
  
  Returns:
  response.text (string): json string with number of rows
  """
  sqlQuery = '''
      SELECT COUNT(*) AS TheCount
      FROM ''' + table_name
      
  payload = json.dumps({
      "query": sqlQuery
  })
  headers = {'Content-Type': 'application/json'}
  response = requests.request("POST", druidUrl, headers=headers, data=payload)
  return response.text
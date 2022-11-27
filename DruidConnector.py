# python 3.8.10 64-bit
try:
  import json
  import requests
except:
  cur = 'Couldn\'t import json or requests library'

def druidConnect(param_host, param_port = "8082") -> 'string':
  """
  Connect to Druid database

	Parameters:
  param_host (string): Druid database host
  param_port (string): Druid broker port | default = "8082"

	Returns:
  url (string): Druid connection url
  """
  try:
    url = "http://" + param_host + ":" + param_port + "/druid/v2/sql/task/"
  except:
    url = 'Couldn\'t connect to Druid broker'
      
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
  response (string): Druid response
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
    
  return response

def druidCreateTableFromCsv(druidUrl, table_name, localPathFolder, csvName, columnList, timestampColumnIndex, partition = 'day') -> 'string':
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
  response (string): Druid response
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
    
  return response
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

def druidInsert(druidUrl, table_name, timestampColName, dataURL, dataFormat, column_list, partition = 'day') -> 'string':
  """
  Create 'insert into' task

  Parameters:
  druidUrl (string): Druid database host URL
  table_name (string): Druid table to insert data to
  timestampColName (string): Data timestamp column
  dataURL (string): Data url location
  dataFormat (string): Data format
  column_list (array): Data columns and their types eg. [colName, colType]
  partition (string): Data partition | default = day

  Returns:
  response (string): Druid response
  """
  sqlQuery = '''INSERT INTO ''' + table_name + '''\n''' 
  sqlQuery += '''SELECT TIME_PARSE(\"''' + timestampColName + '''\") AS __time,\n*\n'''
  sqlQuery += '''FROM TABLE(\nEXTERN(\n'''
  sqlQuery += ''''{\"type\": \"http\", \"uris\": [\"''' + dataURL + '''\"]}',\n'''
  sqlQuery += ''''{\"type\": \"''' + dataFormat + '''\"'''
  sqlQuery += ',"findColumnsFromHeader":true' if (dataFormat == 'csv') else ''
  sqlQuery += '''}',\n'['''

  for column_index, column in enumerate(column_list):
      sqlQuery += '''{\"name\": \"''' + column[0] + '''\",\"type\": \"''' + column[1] + '''\"}'''
      sqlQuery += ',' if (column_index < len(column_list)-1) else ''

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
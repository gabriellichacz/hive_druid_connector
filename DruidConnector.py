# python 3.8.10 64-bit
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
    import json
    import requests
  except:
    cur = 'Couldn\'t import json or requests library'

  try:
    url = "http://" + param_host + ":" + param_port + "/druid/v2/sql/task/"
  except:
    url = 'Couldn\'t connect to Druid broker'
      
  return url

def druidInsert(druidUrl, sqlQuery) -> 'string':
  """
  Create 'insert into' task

  Parameters:
  druidUrl (string): Druid database host URL
  sqlQuery (string): SQL insert query

  Returns:
  response (string): Druid response
  """
  payload = json.dumps({
      "query": sqlQuery,
      "context": {
          "maxNumTasks": 3
      }
  })
  headers = {'Content-Type': 'application/json'}
  response = requests.request("POST", druidUrl, headers=headers, data=payload)

  return response
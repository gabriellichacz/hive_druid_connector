# python 3.8.10 64-bit
def hiveConnect(param_host, param_port, param_username = None, param_auth = 'NONE', param_database = 'default'):
    """
    Connect to Hive database

	Parameters:
		param_host (string): Hive database host
		param_port (string): Hive database port
        param_database (string): Hive database name
        param_auth (string): Hive database password (if it's set)

	Returns:
		Connection cursor
    """
    from pyhive import hive
    conn = hive.Connection(param_host, param_port, param_username, param_auth, param_database)
    cur = conn.cursor()
    return cur

def hiveExecuteSelect(connection, sql):
    """
    Execute Select SQL command in Hive database

	Parameters:
		connection (object): Hive database cursor
		sql (string): SQL statement

	Returns:
		data (string)
    """
    try:
        connection.execute(sql)
        data = connection.fetchall()
    except:
        data = 'Couldn\'t execute SQL statement'
    return data

def hiveExecuteAlteration(connection, sql):
    """
    Execute SQL command in Hive database that doesn't return data (eg. create table)

	Parameters:
		connection (object): Hive database cursor
		sql (string): SQL statement

	Returns:
		Message if sql was executed properly
    """
    try:
        connection.execute(sql)
        message = 'SQL statement executed'
    except:
        message = 'Couldn\'t execute statement'
    return message

def hiveCreateTable(connection, table_name, column_list = [], comment = '', row_format = 'DELIMITED', fields_terminator = '\t', lines_terminator = '\n', stored_as = 'TEXTFILE'):
    """
    Create table in Hive (not tested)

	Parameters:
		connection (object): Hive database cursor
		table_name (string)
        column_list (array): Array with columns' names and types (eg. ['id_column int', 'name_column String'])
        comment (string)
        row_format (string)
        fields_terminator (string)
        lines_terminator (string)
        stored_as (string)

	Returns:
		Message if sql was executed properly
    """
    create_sql = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('

    for column_index, column in enumerate(column_list):
            create_sql += column + ', ' if (column_index < len(column_list)-1) else column

    create_sql += ') COMMENT \'' + comment + '\''
    create_sql += ' ROW FORMAT ' + row_format
    create_sql += ' FIELDS TERMINATED BY \'' + fields_terminator + '\''
    create_sql += ' LINES TERMINATED BY \'' + lines_terminator + '\''
    create_sql += ' STORED AS ' + stored_as

    try:
        hiveExecuteAlteration(connection, create_sql)
        message = 'Table ' + table_name + ' created'
    except:
        message = 'Couldn\'t create ' + table_name + ' table'
    return message

def hiveCreateDruidExternalTable(connection, table_name, druid_table_name, storage_handler = 'org.apache.hadoop.hive.druid.DruidStorageHandler'):
    """
    Create external table in Hive from Druid

	Parameters:
		connection (object): Hive database cursor
		table_name (string)
        druid_table_name (string)
        storage_handler (string) Druid storage handler | default = 'org.apache.hadoop.hive.druid.DruidStorageHandler'

	Returns:
		Message if sql was executed properly
    """
    create_sql = 'CREATE EXTERNAL TABLE IF NOT EXISTS ' + table_name
    create_sql += ' STORED BY \"' + storage_handler + '\"'
    create_sql += ' TBLPROPERTIES ("druid.datasource" = \"' + druid_table_name + '\")'
    
    try:
        hiveExecuteAlteration(connection, create_sql)
        message = 'Table ' + table_name + ' created'
    except:
        message = 'Couldn\'t create ' + table_name + ' table'
    return message

def hiveDropTable(connection, table_name):
    """
    Drops table in Hive

	Parameters:
		connection (object): Hive database cursor
		table_name (string)

	Returns:
		Message if sql was executed properly
    """
    drop_sql = 'DROP TABLE ' + table_name
    
    try:
        hiveExecuteAlteration(connection, drop_sql)
        message = 'Table ' + table_name + ' dropped'
    except:
        message = 'Couldn\'t drop ' + table_name + ' table'
    return message

def hiveSelect(connection, columns, table, where_column = None, where = None):
    """
    Select statement

	Parameters:
		columns (array): columns to select
		table (string) table name
        where_column (string) where statement column | default = None
        where (string) where statement | default = None

	Returns:
		Data
    """
    create_sql = 'SELECT '

    for column_index, column in enumerate(columns):
        create_sql += column + ', ' if (column_index < len(columns)-1) else column

    create_sql += ' FROM ' + table

    if (where_column != None and where != None):
        create_sql += ' WHERE ' + where_column + '= ' + where
    
    try:
        data = hiveExecuteSelect(connection, create_sql)
    except:
        data = 'Couldn\'t execute statement'
    return data
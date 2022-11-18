# python 3.8.10 64-bit
def hiveConnect(param_host, param_port, param_username = None, param_auth = 'NONE', param_database = 'default') -> 'pyhive.hive.Cursor':
    """
    Connect to Hive database

	Parameters:
		param_host (string): Hive database host
		param_port (string): Hive database port
        param_database (string): Hive database name
        param_auth (string): Hive database password (if it's set)

	Returns:
		cur (pyhive.hive.Cursor): Connection cursor
    """
    try:
        from pyhive import hive
    except:
        cur = 'Couldn\'t import pyhive'

    try:
        conn = hive.Connection(param_host, param_port, param_username, param_auth, param_database)
        cur = conn.cursor()
    except:
        cur = 'Couldn\'t connect to Hive'
        
    return cur

def hiveExecuteSelect(connection, sql) -> list:
    """
    Execute Select SQL command in Hive database

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
		sql (string): SQL statement

	Returns:
		data (list)
    """
    try:
        connection.execute(sql)
        data = connection.fetchall()
    except:
        data = 'Couldn\'t execute SQL statement'

    return data

def hiveExecuteAlteration(connection, sql) -> str:
    """
    Execute SQL command in Hive database that doesn't return data (eg. create table)

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
		sql (string): SQL statement

	Returns:
		message (string): Message if sql was executed properly
    """
    try:
        connection.execute(sql)
        message = 'SQL statement executed'
    except:
        message = 'Couldn\'t execute statement'

    return message

def hiveCreateTable(connection, table_name, column_list = [], comment = '', row_format = 'DELIMITED', fields_terminator = '\t', lines_terminator = '\n', stored_as = 'TEXTFILE') -> str:
    """
    Create table in Hive (not tested)

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
		table_name (string)
        column_list (array): Array with columns' names and types (eg. ['id_column int', 'name_column String'])
        comment (string)
        row_format (string)
        fields_terminator (string)
        lines_terminator (string)
        stored_as (string)

	Returns:
		message (string): Message if sql was executed properly
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

def hiveCreateDruidExternalTable(connection, table_name, druid_table_name, storage_handler = 'org.apache.hadoop.hive.druid.DruidStorageHandler') -> str:
    """
    Create external table in Hive from Druid

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
		table_name (string)
        druid_table_name (string)
        storage_handler (string) Druid storage handler | default = 'org.apache.hadoop.hive.druid.DruidStorageHandler'

	Returns:
		message (string): Message if sql was executed properly
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

def hiveDropTable(connection, table_name) -> str:
    """
    Drops table in Hive

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
		table_name (string)

	Returns:
		message (string): Message if sql was executed properly
    """
    drop_sql = 'DROP TABLE ' + table_name
    
    try:
        hiveExecuteAlteration(connection, drop_sql)
        message = 'Table ' + table_name + ' dropped'
    except:
        message = 'Couldn\'t drop ' + table_name + ' table'

    return message

def hiveSelect(connection, columns, table, where_column = None, where = None) -> list:
    """
    Select statement

	Parameters:
        connection (pyhive.hive.Cursor): Hive database cursor
		columns (array): columns to select
		table (string) table name
        where_column (string) where statement column | default = None
        where (string) where statement | default = None

	Returns:
		data (list)
    """
    create_sql = 'SELECT '

    for column_index, column in enumerate(columns):
        create_sql += column + ', ' if (column_index < len(columns)-1) else column

    create_sql += ' FROM ' + table

    if (where_column != None and where != None):
        create_sql += ' WHERE ' + where_column + '= ' + where
    
    data = hiveExecuteSelect(connection, create_sql)

    return data

def insertDataFromTableToTable(connection, tableDruid, tableHive, column_list):
    """
    Insert data from one table to another

	Parameters:
		connection (pyhive.hive.Cursor): Hive database cursor
        tableDruid (string): table name to which the data will be inserted
        tableHive (string): table name from which the data will be taken
		column_list (array): list of columns from tableHive table

	Returns:
		message (string): Message if sql was executed properly
    """
    insert_sql = 'INSERT INTO ' + tableDruid
    insert_sql += ' SELECT '
    for column_index, column in enumerate(column_list):
            insert_sql += column + ', ' if (column_index < len(column_list)-1) else column
    insert_sql += ' FROM ' + tableHive

    connection.execute(insert_sql)
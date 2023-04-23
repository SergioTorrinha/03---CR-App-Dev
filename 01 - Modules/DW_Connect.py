#                                                                           #       
#                       SQL Data Base Connector Module                      #
#                                                                           #

import os
import pandas as pd
import pyodbc


def cred_reader(filename):
    """
        Objetive: 
        -------
        This function returns a pandas dataframe with the log in credential elements
        extracted from a .txt file.

        Inputs:
        ------
        A .txt file with database log in credentials.
        It was assumed this file has the following structure:

        server = <database server name>
        database = <database name>
        username = <user name>
        password = <password>        
    """
    #check if filename is a valid file path. 
    isFile = os.path.isfile(filename)
    assert isFile, 'Not a valid file!'
    
    df = pd.read_csv(filename, header=None)
    df[['element','value']] = df[0].str.split(' = ', expand=True)
    df = df[['element','value']]
    df['element'] = df['element'].str.strip()
    df['value'] = df['value'].str.strip()
    
    return df


def dwConnector (sv=None, db=None, un=None, pw=None, cr=None):
    """
        Objetive: Creates an SQL database connector, using pyodbc package.
        -------
        
        Inputs:
        ------        
        sv - server name
        db - database name
        un - username
        pw - password

        cr - dataframe with the credentials (created from cred_reader function above)
    """
    assert isinstance(sv, str) or sv == None, 'Invalid data provided.'
    assert isinstance(db, str) or db == None, 'Invalid data provided.'
    assert isinstance(un, str) or un == None, 'Invalid data provided.'
    assert isinstance(pw, str) or pw == None, 'Invalid data provided.'
    
    #checks if single credentials or if a credential reader were passed into the function
    if sv==None and db==None and un==None and pw==None and not cr.empty:
        server      = cr[cr.element=='server'   ]['value'].unique()[0]
        database    = cr[cr.element=='database' ]['value'].unique()[0]
        username    = cr[cr.element=='username' ]['value'].unique()[0]
        password    = cr[cr.element=='password' ]['value'].unique()[0]

        driver= '{ODBC Driver 17 for SQL Server}'
        cn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        return cn

    elif not sv==None and not db==None and not un==None and not pw==None and cr.empty:
        server      = sv
        database    = db
        username    = un
        password    = pw

        driver= '{ODBC Driver 17 for SQL Server}'
        cn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
        return cn

    else:
        print('Please check your credentials or credential reader again!')


def sql_query(query_string, connector):
    """
        Objetive: Return a dataframe with the result of the query string passedby the user.
        --------

        Inputs:
        -----
        query_string    - string that resembles a sql query or a string that points to the location of a sql file
        connector       - connector to the database 
    """
    assert isinstance( query_string, str ) or query_string.lower().endswith('.sql'), 'Please provide a valid query string or a valid .sql file to read.'
    #to improve: how to assert if connector is a valid sql connector?

    if query_string.lower().endswith('.sql'):
        # Read the sql file
        query_string = open( query_string, 'r')        
        df = pd.read_sql_query( query_string.read(), connector )
        query_string.close()
    else:
        #Reads the query string
        df = pd.read_sql_query( query_string, connector )

    #Formats dates, if DimDateID exists in the dataframe
    # In some tables, the date format is different then the one present in DimDateID field.
    # Therefore, this formulation can be further improved, to acomodate those cases, if we find it necessary in future. 
    if 'DimDateID' in df.columns: 
        df['DimDateID'] = pd.to_datetime( pd.to_numeric( df['DimDateID'] ) + 1, format='%Y%m%d' ) 

    return df
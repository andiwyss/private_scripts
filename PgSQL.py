#-------------------------------------------------------------------------------
# Name:        PgSQL.py
# Purpose:
#
# Author:      Andi Wyss
#
# Created:     07.11.2016
# History:     1.0 created
# Copyright:   (c) Andi Wyss 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import psycopg2 as pgsql
import psycopg2.extras as pgsqlxtra
import logging

# register uuid functions
pgsqlxtra.register_uuid()

class PgSQL(object):
    """
    Get the ability to connect a PostgreSQL Database.
    Connection properties: dbname, user, password
    Available functions:
        connect (database name)
        open cursor (_connection)
        select query (SELECT field FROM table WHERE whereClause)
        disconnect (connection)
    """

    def __init__(self,databaseName,user,password):
        self.dbname = databaseName
        self.user = user #"admin"
        self.password = password #"?|\/btH|Uf"

    def connectDB(self):
        """Connecting dbname
           returns a connection"""
        connection = pgsql.connect(dbname=self.dbname, user=self.user, password=self.password)
        return connection
    #

    def openCursor(self, connection):
        """Opens a cursor in connection"""
        cursor = connection.cursor()
        return cursor
    #

    def selectQry(self, in_cursor,fieldName,table,whereClause=None):
        """Executes a select query and returns result as list with dict/row
           SELECT * FROM * WHERE *"""

        out_result = []

        # extract fieldnames
        fieldNames = []
        fieldValues = [name.strip() for name in fieldName.split(',')]
        for value in fieldValues:
            if len(value) > 1:
                name = value.split('as')[-1].strip()
                fieldNames.append(name)
            else:
                fieldNames.append(value)

        # format and executing statement
        if not whereClause:
            stmt = "SELECT {} FROM {}".format(fieldName,table)
        else:
            stmt = "SELECT {} FROM {} WHERE {}".format(fieldName,table,whereClause)
        logging.debug('Executing Statement: {}'.format(stmt))
        in_cursor.execute(stmt)
        result=in_cursor.fetchall()

        # get count of returned rows
        resultCount = len(result)

        # format output
        for row in result:
            resultRow = {}
            i = 0
            while i < len(fieldNames):
                resultRow[fieldNames[i]]=list(row)[i]
                i += 1
            out_result.append(resultRow)

        return out_result, resultCount
    #

    def deleteQry(self, in_cursor, table, whereClause):
        """Executes a delete statement
           DELETE FROM * WHERE *"""

        stmt = "DELETE FROM {} WHERE {}".format(table,whereClause)
        logging.debug('Executing Statement: {}'.format(stmt))
        in_cursor.execute(stmt)
    #

    def updateRow(self, in_cursor, table, updateField, updateValue, whereClause):
        """Executes a update statement
           UPDATE <table> SET <updateField> = <updateValue> WHERE <whereClause>"""
        stmt = 'UPDATE {} SET {} = {} WHERE {};'.format(table,updateField,updateValue,whereClause)
        logging.debug('Executing Statement: {}'.format(stmt))
        in_cursor.execute(stmt)


    def disconnect(self, connection):
        """Disconnect connection"""
        connection.close()
    #


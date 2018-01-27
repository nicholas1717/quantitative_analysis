#!/usr/local/bin/python3
import pymysql

class MysqlConnector:
    def __init__( self, phost, pport, pusr, ppwd):
        self.__connector = pymysql.connect(host=phost,port=int(pport),user=pusr,passwd=ppwd)
        self.__cursor = self.__connector.cursor()

    def __del__( self):
        self.__connector.close()

    def CreateDB( self, pdbname):
        try:
            self.__cursor.execute( "DROP DATABASE IF EXISTS " + pdbname + ";")
            self.__cursor.execute( "CREATE DATABASE " + pdbname + ";")
        except:
            print ("Error: unable to create db")

    def SetDB( self, pdbname):
        try:
            self.__cursor.execute( "USE " + pdbname + ";")
        except:
            print("Error: unable to set table")

    def SetTemplate( self, ptemplate):
        try:
            self.__cursor.execute( ptemplate)
        except:
            print("Error: unable to create table")

    def CreateTables( self, ptables):
        try:
            for tb in ptables:\
                self.__cursor.execute( "CREATE TABLE IF NOT EXISTS " + tb + " LIKE tb_template;")
        except Exception as e:
            print(e)
            print("Error: unable to create table")

    def DropTables( self, ptables):
        try:
            for tb in ptables:
                self.__cursor.execute( "DROP TABLE IF EXISTS " + tb + ";")
        except:
            print("Error: unable to drop table")

    def SelectOne( self, psql):
        try:
            self.__cursor.execute(psql)
            results = self.__cursor.fetchone()
            return results
        except:
            print ("Error: unable to fetch data")

    def SelectAll( self, psql):
        try:
            self.__cursor.execute(psql)
            results = self.__cursor.fetchall()
            return results
        except:
            print ("Error: unable to fetch data")

    def Insert( self, psql):
        try:
            self.__cursor.execute(psql)
            self.__connector.commit()
        except:
            self.__connector.rollback()
            print ("Error: unable to insert data")

    def InsertBatch( self, psql):
        try:
            for p in psql:
                self.__cursor.execute(p)
            self.__connector.commit()
        except Exception as e:
            print(e)
            self.__connector.rollback()
            print ("Error: unable to insert data")

    def Update( self, psql):
        try:
            self.__cursor.execute(psql)
            self.__connector.commit()
        except:
            self.__connector.rollback()
            print ("Error: unable to update data")

    def Delete( self, psql):
        try:
            self.__cursor.execute(psql)
            self.__connector.commit()
        except:
            self.__connector.rollback()
            print ("Error: unable to delete data")
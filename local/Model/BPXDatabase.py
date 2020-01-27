"""
***************************************************************************************
   Description: A class/set of methods that simplify the connection to various databases
   in order to faciliate fast data collection
   ***********************************************************************************
   Input Parameters:   | N/A
   Output Parameters:  | N/A
   Tables Accessed:    | N/A
   Tables Affected:    | N/A
   ----------------------------------------------------------------------------------
                                  Version Control
   ----------------------------------------------------------------------------------
   Version    Developer   Date       Change
   -------    ---------   ---------- ------------------------------------------------
   1.0        Travis C    06/20/2019 Initial Creation
***************************************************************************************
"""

##Class for creating a database object
import pyodbc
import pandas as pd

def GetDBEnvironment(input, UID):
    if input == 'TestEDH':
        ret = BPXDatabase('sqldwtest.database.windows.net', 'EnterpriseDataHub', UID),
    elif input == 'TestODS':
        ret = BPXDatabase('sqldwtest.database.windows.net', 'ODS', UID)
    elif input == 'TestEDW':
        ret = BPXDatabase('sqldwtest.database.windows.net', 'EDW', UID)
    elif input == 'TestTOE':
        ret = BPXDatabase('sqldwtest.database.windows.net', 'TeamOptimizationEngineering', UID)
    elif input == 'StageEDH':
        ret = BPXDatabase('sqldwstage.database.windows.net', 'EnterpriseDataHub', UID)
    elif input == 'StageODS':
        ret = BPXDatabase('sqldwstage.database.windows.net', 'ODS', UID)
    elif input == 'StageEDW':
        ret = BPXDatabase('sqldwstage.database.windows.net', 'EDW', UID)
    elif input == 'ProdEDH':
        ret = BPXDatabase('sqldwprod.database.windows.net', 'EnterpriseDataHub', UID)
    elif input == 'ProdODS':
        ret = BPXDatabase('sqldwprod.database.windows.net', 'ODS', UID)
    elif input == 'ProdEDW':
        ret = BPXDatabase('sqldwprod.database.windows.net', 'EDW', UID)
    elif input == 'OnPrem':
        ret = BPXDatabase('10.75.6.160', '', '')
    elif input == 'ProdDSO':
        ret = BPXDatabase('coe-w-act-prod-sqlsrv.database.windows.net', 'ActenumDSO', UID)
    else:
        ret = 'Invalid'
    return ret

def GetTSEnvironment(server):
    return ''

class BPXDatabase:
    def __init__(self, servername, database, UID):
        self.server = servername
        self.database = database
        self.UID = UID
        driver, drivermessage = self.__GetDrivers()
        connectionList, messages = self.__Connect(driver, drivermessage)
        self.conn = connectionList[0]
        self.cursor = connectionList[1]
        self.messages = messages
        self.ValidConnection = self.__ConnectionValid()

    def __Connect(self, driver, drivermessage): 
        if 'N/A' in driver:
            messages = drivermessage
            conn = 'N/A'
            cursor = 'N/A'
        else:
            if self.UID == 'OVERRIDE':
                EnterpriseDataHub_username ='svcArrowReader' #os.environ['EnterpriseDataHub_username']
                EnterpriseDataHub_password ='mbdu6F1N7TUeLzTSwn' #os.environ['EnterpriseDataHub_password']
                connectionstring = 'Driver={' + driver + '}; Server=' + self.server + '; Port=1443; Database=' + self.database + '; UID='+EnterpriseDataHub_username + \
                '; PWD='+EnterpriseDataHub_password+ ';'
            elif self.server == '10.75.6.160':
                username = 'spotfirereader'
                password = 'Spotf1re'
                connectionstring = 'Driver={' + driver + '}; Server=' + self.server + '; Port=1443; Database=' + self.database + '; UID=' + username + \
                '; PWD='+ password+ ';'
            elif self.server == 'localhost':
                connectionstring = 'Driver={' + driver + '}; Server=' + self.server + '; Port=1443; Database=' + self.database + '; UID='+self.UID+\
                '; Trusted_Connection=yes;'
            else:
                connectionstring = 'Driver={' + driver + '}; Server=' + self.server + '; Port=1443; Database=' + self.database + '; UID='+self.UID+\
                '; Authentication=ActiveDirectoryInteractive;'
            try:
                conn = pyodbc.connect(connectionstring)                            
                cursor = conn.cursor()
                messages = "Success"
            except Exception as e:
                conn = 'N/A'
                cursor = "N/A"
                messages = "Error Occurred: " + str(e)
            connectionList = (conn, cursor)
        return connectionList, messages

    def Query(self, command):        
        rows = self.cursor.execute(command)             
        columns = [column[0] for column in rows.description]
        results = []
        all_rows = rows.fetchall()
        for row in all_rows:
            results.append(dict(zip(columns, row)))

        #df = pd.DataFrame([[getattr(i,j) for j in columns] for i in results], columns = columns)
        df = pd.DataFrame(results)
        return results, df
    def Command(self, command):
        Messages = ''
        Success = True
        try:
            self.cursor.execute(command) 
        except Exception as e:
            Messages = str(e)
            Success = False

        return Success, Messages
    def Disconnect(self):
        self.cursor.close()

    def __ConnectionValid(self):
        value = False
        if isinstance(self.conn, pyodbc.Connection) and isinstance(self.cursor, pyodbc.Cursor):
            value = True
        return value

    def __GetDrivers(self):
        #In order to use BPX's Integrated Active Directory login, the user must have ODBC Driver 13.1 or higher
        import re
        drivers = pyodbc.drivers()
        ODBCDrivers = []
        #Move ODBC drivers into list
        for driver in drivers:
            if "ODBC Driver" in driver:
                version = re.findall( r'[\d\.]{2,}|\d+', driver)
                if float(version[0]) >= 13:
                    ODBCDrivers.append(driver)                
            
        if len(ODBCDrivers) > 0:
            ODBCDrivers.sort()
            retDriver = ODBCDrivers[len(ODBCDrivers) - 1]            
            messages = 'Success'         
        else:
            messages = 'No ODBC Driver Found above version 13.1'
            retDriver = 'N/A'
        return retDriver, messages



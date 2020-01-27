#DB Configuration
def GetConfig():
    config = {'server' : 'localhost',         
              'database' : 'LEForecastDatabase',
              'UID' : '',
              'password' : ''}

    return config

def ValidateAndClauseArguments(kw_dict, table_name, DBobj):
    from Model import BPXDatabase as bpx
    from Model import QueryFile as qf

    col_query = qf.ColumnQuery(table_name)
    results = DBobj.Query(col_query)

    col_list = results[1]['column_name'].to_list()
    clause = []
    for key, value in kw_dict.items():
        if key in col_list:
            in_clause = AddInClause(value)
            clause.append(key + ' in ' + in_clause)

    if kw_dict:
        stmt = 'where '
        count = 1
        for item in clause:
            if count == 1:
                stmt = stmt + item
            else:
                stmt = stmt + ' and ' + item
            count = count + 1
    else:
        stmt = ''

    return stmt

def AddInClause(item_list):    
    
    count = 1
    ret = '('
    for item in item_list:
        if count != len(item_list):
            ret = ret + '\'' + str(item) + '\', '
        else:
            ret = ret + '\'' + str(item) + '\')'
        count = count + 1

    return ret

def ReadFromTables(DBObj, table_name, where_clause):
    from Model import BPXDatabase as bpx

    #Check the where_Clause to make sure it is not empty:
    # before, after = str.split(where_clause, 'where ')

    # if not after:
    #     where_clause = ''

    #Form basic select statement

    stmt = 'select * from ' + table_name + ' ' + where_clause
    results = DBObj.Query(stmt)

    return results[1]

class ForecastHeader:
    def __init__(self, DBObj, WellName=[], CorpID=[], ForecastName=[]):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.table = 'Forecast_Header'
        self.WellName = WellName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.ForecastName = ForecastName

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.WellName:
                where_dict['WellName'] = self.WellName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.ForecastName:
                where_dict['ForecastName'] = self.ForecastName

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            header_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            #Convert df to table row objects
            rows = []
            for idx, item in header_df.iterrows():
                Arps_Dict = {}
                Arps_Dict['b'] = item['DCA_b']
                Arps_Dict['qi'] = item['DCA_qi']
                Arps_Dict['Di'] = item['DCA_Di']
                row = m.ForecastHeaderRow(item['WellName'], item['CorpID'], item['ForecastName'], item['GFOzYear'], item['Aries_ScenarioID'], Arps_Dict, 
                item['GFOz'], self.DBObj)
                rows.append(row)
            
        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class ForecastHeaderRow:
    def __init__(self, WellName, CorpID, ForecastName, ForecastYear, scenarioName, Arps, GFO,  DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.WellName = WellName
        self.CorpID = CorpID
        self.ForecastName = ForecastName
        self.ForecastYear = ForecastYear
        self.scenarioName = scenarioName
        if Arps:
            self.Di = str(Arps['Di'])
            self.qi = str(Arps['qi'])
            self.b = str(Arps['b'])
        else:
            self.Di = ''
            self.qi = ''
            self.b = ''
        if GFO:
            self.GFO = GFO
        else:
            self.GFO = False

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
    
    def Write(self, Update_User, Update_Date):
        from datetime import datetime 
        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            hdr_insert_statement = 'insert into [LEForecastDatabase].[dbo].[Forecast_Header] (WellName, CorpID, ForecastName, GFOz, \n'\
                'GFOzYear, Aries_ScenarioID, DCA_Di, DCA_qi, DCA_b,  Update_Date, Update_User)\n'\
                ' values (\'' + self.WellName + '\', \'' + self.CorpID + '\', \'' + self.ForecastName + '\', \'' + str(self.GFO) + '\',\n'\
                '\'' + str(self.ForecastYear) + '\', \'' + self.scenarioName + '\', \'' + str(self.Di) + '\', \'' + str(self.qi) + '\', \'' + str(self.b) + '\'\n'\
                ', convert(datetime, \'' + Update_Date + '\', 120), \'' + Update_User + '\')'
                
            Success, Message = self.DBObj.Command(hdr_insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error writing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Forecast Header table, Query the table for existing entry
            #ForecastName, CorpID
            ForecastHeaderObj = m.ForecastHeader(self.DBObj, [], [self.CorpID], [self.ForecastName])
            rows, Success, Message = ForecastHeaderObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of header.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Forecast Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        Success = True
        Messages = []

        try:
            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[Forecast_Header] where ForecastName = \'' + self.ForecastName + '\' and CorpID = \'' + self.CorpID + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages

class ForecastData:
    def __init__(self, DBObj, HeaderName=[], CorpID=[], Date_Key = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'Forecast_Data'
        self.HeaderName = HeaderName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.Date_Key = Date_Key

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd
        from datetime import datetime 

        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.HeaderName:
                where_dict['HeaderName'] = self.HeaderName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.Date_Key:
                if isinstance(self.Date_Key[0], datetime):
                    self.Date_Key[0] = self.Date_Key[0].strftime('%Y-%m-%d %H:%M:%S')
                where_dict['Date_Key'] = self.Date_Key

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.ForecastDataRow(item['HeaderName'] , item['CorpID'], item['Date_Key'], item['Gas_Production'], item['Oil_Production'], item['Water_Production'], 
                item['GasNettingFactor'], item['OilNettingFactor'], item['WaterNettingFactor'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class ForecastDataRow:
    def __init__(self, HeaderName, CorpID, Date_Key, Gas_Production, Oil_Production, Water_Production, GasNF, OilNF, WaterNF,  DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.HeaderName = HeaderName
        self.CorpID = CorpID
        self.Date_Key = Date_Key
        self.Gas_Production = Gas_Production
        self.Oil_Production = Oil_Production
        self.Water_Production = Water_Production
        if GasNF:            
            self.GasNF = GasNF
        else:
            self.GasNF = 0       
        if OilNF:            
            self.OilNF = OilNF
        else:
            self.OilNF = 0    
        if WaterNF:            
            self.WaterNF = WaterNF
        else:
            self.WaterNF = 0               
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            if not self.Gas_Production:
                self.Gas_Production = 0
            if not self.Oil_Production:
                self.Oil_Production = 0
            if not self.Water_Production:
                self.Water_Production = 0
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[Forecast_Data] (HeaderName, CorpID, Date_Key, Gas_Production, Oil_Production, Water_Production, '\
                'GasNettingFactor, OilNettingFactor, WaterNettingFactor, Update_Date, Update_User)'\
                ' values (\'' +  self.HeaderName + '\', \'' + self.CorpID + '\', convert(datetime, \'' +  self.Date_Key + '\', 120) , ' +  str(self.Gas_Production) + ',\n'\
                '' + str(self.Oil_Production) + ', ' + str(self.Water_Production) + ', ' + str(self.GasNF) + ', ' + str(self.OilNF) + ', ' + str(self.WaterNF) + ', '\
                ' convert(datetime, \'' + Update_Date + '\', 120), \'' +  Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error writing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Forecast Header table, Query the table for existing entry
            #HeaderName, CorpID, Date_Key
            ForecastDataObj = m.ForecastData(self.DBObj, [self.HeaderName], [self.CorpID], [self.Date_Key])
            rows, Success, Message = ForecastDataObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Forecast Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        Success = True
        Messages = []
        from datetime import datetime

        try:
            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[Forecast_Data] where HeaderName = \'' + self.HeaderName + '\' and CorpID = \'' + self.CorpID + '\' and Date_Key = \'' + self.Date_Key + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages        

class LEHeader:
    def __init__(self, DBObj, WellName=[], CorpID=[], LEName=[], LE_Date = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.table = 'LE_Header'
        self.WellName = WellName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.LEName = LEName
        self.LE_Date = LE_Date

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.WellName:
                where_dict['WellName'] = self.WellName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.LEName:
                where_dict['LEName'] = self.LEName
            if self.LE_Date:
                where_dict['LE_Date'] = self.LE_Date

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            header_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            #Convert df to table row objects
            rows = []
            for idx, item in header_df.iterrows():                
                row = m.LEHeaderRow(item['LEName'], item['WellName'], item['CorpID'], item['ForecastGeneratedFrom'],  item['Wedge'], item['LE_Date'], self.DBObj)
                rows.append(row)
            
        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class LEHeaderRow:
    def __init__(self, LEName, WellName, CorpID, ForecastGeneratedFrom, Wedge, LE_Date, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.LEName = LEName
        self.CorpID = CorpID
        self.ForecastGeneratedFrom = ForecastGeneratedFrom
        self.WellName = WellName
        self.Wedge = Wedge
        self.LE_Date = LE_Date
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime
        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.LE_Date, datetime):
                self.LE_Date = self.LE_Date.strftime('%Y-%m-%d %H:%M:%S')
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[LE_Header] (LEName, WellName, CorpID, ForecastGeneratedFrom, Wedge, LE_Date, Update_User, Update_Date) \n'\
                                'values (\'' + self.LEName + '\', \'' + self.WellName + '\', \'' + self.CorpID + '\', \'' + self.ForecastGeneratedFrom + '\', \'' + self.Wedge + '\', convert(datetime, \'' + self.LE_Date + '\', 120), \'' + Update_User + '\', convert(datetime,\'' + Update_Date + '\', 120))'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for LE Header table, Query the table for existing entry
            
            LEHeaderObj = m.LEHeader(self.DBObj, [self.WellName], [self.CorpID], [self.LEName], [self.LE_Date])
            rows, Success, Message = LEHeaderObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Forecast Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        Success = True
        Messages = []

        #To Do - Delete all rows associated with this header as well
        try:
            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[LE_Header] where LEName = \'' + self.LEName + '\' and CorpID = \'' + self.CorpID + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages   

class LEData:
    def __init__(self, DBObj, HeaderName=[], CorpID=[], Date_Key = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'LE_Data'
        self.HeaderName = HeaderName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.Date_Key = Date_Key

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        import pandas as pd
        from Model import ModelLayer as m
        from datetime import datetime

        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.HeaderName:
                where_dict['HeaderName'] = self.HeaderName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.Date_Key:
                if isinstance(self.Date_Key[0], datetime):
                    self.Date_Key[0] = self.Date_Key[0].strftime('%Y-%m-%d %H:%M:%S')
                where_dict['Date_Key'] = self.Date_Key

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.LEDataRow(item['HeaderName'] , item['CorpID'], item['Date_Key'], item['Gas_Production'], item['Oil_Production'], item['Water_Production'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class LEDataRow:
    def __init__(self, HeaderName, CorpID, Date_Key, Gas_Production, Oil_Production, Water_Production, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.HeaderName = HeaderName
        self.CorpID = CorpID
        self.Date_Key = Date_Key
        self.Gas_Production = Gas_Production
        self.Oil_Production = Oil_Production
        self.Water_Production = Water_Production
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            if not self.Gas_Production:
                self.Gas_Production = 0
            if not self.Oil_Production:
                self.Oil_Production = 0
            if not self.Water_Production:
                self.Water_Production = 0
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[LE_Data] (HeaderName, CorpID, Date_Key, Gas_Production, Oil_Production, Water_Production, Update_Date, Update_User)'\
                ' values (\'' +  self.HeaderName + '\', \'' + self.CorpID + '\', convert(datetime, \'' +  self.Date_Key + '\', 120) , ' +  str(self.Gas_Production) + ',\n'\
                '' + str(self.Oil_Production) + ', ' + str(self.Water_Production) + ',  convert(datetime, \'' + Update_Date + '\', 120), \'' +  Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error writing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for LE Data table, Query the table for existing entry
            LEDataObj = m.LEData(self.DBObj, [self.HeaderName], [self.CorpID], [self.Date_Key])
            rows, Success, Message = LEDataObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the LE Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []    

        try:
            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[LE_Data] where HeaderName = \'' + self.HeaderName + '\' and CorpID = \'' + self.CorpID + '\' and Date_Key = \'' + self.Date_Key + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages        

class GasNetting:
    def __init__(self, DBObj, WellName=[], CorpID=[], NettingDate = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'GasNettingValues'
        self.WellName = WellName
        self.NettingDate = NettingDate
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.WellName:
                where_dict['WellName'] = self.WellName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.NettingDate:
                where_dict['NettingDate'] = self.NettingDate

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.GasNettingRow(item['WellName'] , item['CorpID'], item['NettingValue'], item['NettingDate'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class GasNettingRow:
    def __init__(self, WellName, CorpID, NettingValue, NettingDate, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.WellName = WellName
        self.CorpID = CorpID
        self.NettingValue = NettingValue
        self.NettingDate = NettingDate

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.NettingDate, datetime):
                self.NettingDate = self.NettingDate.strftime('%Y-%m-%d %H:%M:%S')
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[GasNettingValues] (WellName, CorpID, NettingValue, NettingDate, Update_Date, Update_User) values \n'\
                '(\'' + self.WellName + '\', \'' + self.CorpID + '\', \'' + str(self.NettingValue) + '\', \'' + self.NettingDate + '\', \'' + Update_Date + '\', \'' + Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error witing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Netting Values table, Query the table for existing entry
            
            NettingObj = m.GasNetting(self.DBObj, [self.WellName], [self.CorpID], [self.NettingDate])
            rows, Success, Message = NettingObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Netting Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []

        try:
            if isinstance(self.NettingDate, datetime):
                self.NettingDate = self.NettingDate.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[GasNettingValues] where WellName = \'' + self.WellName + '\' and CorpID = \'' + self.CorpID + '\' and NettingDate = \'' + self.NettingDate + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages       

class OilNetting:
    def __init__(self, DBObj, WellName=[], CorpID=[], NettingDate = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'OilNettingValues'
        self.WellName = WellName
        self.CorpID = CorpID
        self.NettingDate = NettingDate
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.WellName:
                where_dict['WellName'] = self.WellName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.NettingDate:
                where_dict['NettingDate'] = self.NettingDate

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.GasNettingRow(item['WellName'] , item['CorpID'], item['NettingValue'], item['NettingDate'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class OilNettingRow:
    def __init__(self, WellName, CorpID, NettingValue, NettingDate, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.WellName = WellName
        self.CorpID = CorpID
        self.NettingValue = NettingValue
        self.NettingDate = NettingDate

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.NettingDate, datetime):
                self.NettingDate = self.NettingDate.strftime('%Y-%m-%d %H:%M:%S')
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[OilNettingValues] (WellName, CorpID, NettingValue, NettingDate, Update_Date, Update_User) values \n'\
                '(\'' + self.WellName + '\', \'' + self.CorpID + '\', \'' + str(self.NettingValue) + '\', \'' + self.NettingDate + '\', \'' + Update_Date + '\', \'' + Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error witing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Netting Values table, Query the table for existing entry
            
            NettingObj = m.OilNetting(self.DBObj, [self.WellName], [self.CorpID], [self.NettingDate])
            rows, Success, Message = NettingObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Netting Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []

        try:
            if isinstance(self.NettingDate, datetime):
                self.NettingDate = self.NettingDate.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[OilNettingValues] where WellName = \'' + self.WellName + '\' and CorpID = \'' + self.CorpID + '\' and NettingDate = \'' + self.NettingDate + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages  

class LESummary:
    def __init__(self, DBObj, SummaryName=[], Wedge=[], LEName = [], GFOForecastName = []):
        from Model import BPXDatabase as bpx
        import pandas as pd
        from Model import ModelLayer as m

        self.table = 'LE_Summary'
        self.SummaryName = SummaryName
        self.Wedge = Wedge
        self.LEName = LEName
        self.GFOForecastName = GFOForecastName
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        import pandas as pd
        from Model import ModelLayer as m

        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.SummaryName:
                where_dict['SummaryName'] = self.SummaryName
            if self.LEName:
                where_dict['LEName'] = self.LEName
            if self.Wedge:
                where_dict['Wedge'] = self.Wedge
            if self.GFOForecastName:
                where_dict['GFOForecastName'] = self.GFOForecastName

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.LESummaryRow(item['SummaryName'], item['Wedge'], item['Midstream'], item['Reason'], item['Comments'], item['SummaryDate'], item['LEName'], item['GFOForecastName'], item['MonthlyAvgMBOED'], item['QuarterlyAvgMBOED'], 
                        item['AnnualAvgMBOED'], item['MonthlyGFOMBOED'], item['QuarterlyGFOMBOED'], item['AnnualGFOMBOED'], item['MonthlyVariance'], item['QuarterlyVariance'], item['AnnualVariance'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class LESummaryRow:
    def __init__(self, SummaryName, Wedge, Midstream, Reason, Comments, SummaryDate, LEName, GFOForecastName, MonthlyAvgMBOED, QuarterlyAvgMBOED, 
    AnnualAvgMBOED, MonthlyGFOMBOED, QuarterlyGFOMBOED, AnnualGFOMBOED, MonthlyVariance, QuarterlyVariance, AnnualVariance, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.SummaryName = SummaryName
        self.Wedge = Wedge
        self.Midstream = Midstream
        self.Reason = Reason
        self.Comments = Comments
        self.SummaryDate = SummaryDate
        self.LEName= LEName
        self.GFOForecastName = GFOForecastName
        self.MonthlyAvgMBOED = MonthlyAvgMBOED
        self.QuarterlyAvgMBOED = QuarterlyAvgMBOED
        self.AnnualAvgMBOED = AnnualAvgMBOED
        self.MonthlyGFOMBOED = MonthlyGFOMBOED
        self.QuarterlyGFOMBOED = QuarterlyGFOMBOED
        self.AnnualGFOMBOED = AnnualGFOMBOED
        self.MonthlyVariance = MonthlyVariance
        self.QuarterlyVariance = QuarterlyVariance
        self.AnnualVariance = AnnualVariance

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.SummaryDate, datetime):
                self.SummaryDate = self.SummaryDate.strftime('%Y-%m-%d %H:%M:%S')
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[LE_Summary] (SummaryName, Wedge, Midstream, Reason, Comments, SummaryDate, \n'\
               ' LEName, GFOForecastName, MonthlyAvgMBOED, QuarterlyAvgMBOED, AnnualAvgMBOED, MonthlyGFOMBOED, QuarterlyGFOMBOED, AnnualGFOMBOED, \n'\
               ' MonthlyVariance, QuarterlyVariance, AnnualVariance, Update_Date, Update_User ) values \n'\
                '(\'' + self.SummaryName + '\', \'' + self.Wedge + '\', \'' + self.Midstream + '\', \'' + self.Reason + '\', \'' + self.Comments + '\', \'' + self.SummaryDate + '\',\n'\
                ' \'' + self.LEName + '\', \'' + self.GFOForecastName  + '\', \n'\
                '\'' + str(self.MonthlyAvgMBOED) + '\', \'' + str(self.QuarterlyAvgMBOED) + '\', \'' + str(self.AnnualAvgMBOED) + '\', '\
                '\'' + str(self.MonthlyGFOMBOED) + '\', \'' + str(self.QuarterlyGFOMBOED) + '\', \'' + str(self.AnnualGFOMBOED) + '\', \'' + str(self.MonthlyVariance) + '\', '\
                '\'' + str(self.QuarterlyVariance) + '\', \''+ str(self.AnnualVariance) + '\', \'' + Update_Date + '\', \'' + Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error writing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for LE table, Query the table for existing entry
            LESummaryObj = m.LESummary( self.DBObj, [self.SummaryDate], [self.WellName], [self.CorpID], [self.Area])
            rows, Success, Message = LESummaryObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Summary table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        Success = True
        Messages = []

        try:

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[LE_Summary] where SummaryName = \'' + self.SummaryName + '\' and CorpID = \'' + self.CorpID + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages   

class FracHitMultipliers:
    def __init__(self, DBObj, LEName=[], CorpID=[], Date_Key = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'Frac_Hit_Multipliers'
        self.LEName = LEName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.Date_Key = Date_Key

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.LEName:
                where_dict['LEName'] = self.LEName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.Date_Key:
                where_dict['Date_Key'] = self.Date_Key

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.FracHitMultipliersRow(item['LEName'] , item['CorpID'], item['Date_Key'], item['Multiplier'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class FracHitMultipliersRow:
    def __init__(self, LEName, CorpID, Date_Key, Multiplier, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.LEName = LEName
        self.CorpID = CorpID
        self.Date_Key = Date_Key
        self.Multiplier = Multiplier

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')
            
            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')
            
            if not isinstance(self.Multiplier, str):
                self.Multiplier = str(self.Multiplier)

            insert_statement = 'insert into [LEForecastDatabase].[dbo].[Frac_Hit_Multipliers] (LEName, CorpID, Date_Key, Multiplier, Update_Date, Update_User ) values \n'\
                '(\'' + self.LEName + '\', \'' + self.CorpID + '\', \'' + self.Date_Key + '\', \'' + self.Multiplier + '\', \'' + Update_Date + '\', \'' + Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Multipliers table, Query the table for existing entry
            
            FracHitMultipliersObj = m.FracHitMultipliers(self.DBObj, [self.LEName], [self.CorpID], [self.Date_Key])
            rows, Success, Message = FracHitMultipliersObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Forecast Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []

        try:
            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[Frac_Hit_Multipliers] where LEName = \'' + self.LEName + '\' and CorpID = \'' + self.CorpID + '\' and Date_Key = \'' + self.Date_Key + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages  

class AreaAggregation:
    def __init__(self, DBObj, AggregateName = [], WellNames = [], CorpIDs = []):
        from Model import ModelLayer as m
        from Model import BPXDatabase as bpx

        self.AggregateName = AggregateName
        self.WellNames = WellNames
        self.CorpIDs = CorpIDs
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.table = 'AreaAggregation'

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd
        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.AggregateName:
                where_dict['AggregateName'] = self.AggregateName
            if self.WellNames:
                where_dict['WellName'] = self.WellNames
            if self.CorpIDs:
                where_dict['CorpID'] = self.CorpIDs

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.AreaAggregationRow(item['AggregateName'] , item['WellName'], item['CorpID'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class AreaAggregationRow:
    def __init__(self, AggregateName, WellName, CorpID, DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.AggregateName = AggregateName
        self.WellName = WellName
        self.CorpID = CorpID

        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            insert_statement = 'insert into [LEForecastDatabase].[dbo].[AreaAggregation] (AggregateName, WellName, CorpID, Update_Date, Update_User ) values \n'\
                '(\'' + self.AggregateName + '\', \'' + self.WellName + '\', \'' + self.CorpID + '\', \'' + Update_Date + '\', \'' + Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for Aggregation Area table, Query the table for existing entry
            
            AreaAggregationObj = m.AreaAggregation(self.DBObj, [self.LEName], [self.CorpID], [self.Date_Key])
            rows, Success, Message = AreaAggregationObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Forecast Data table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []

        try:
            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[AreaAggregation] where AggregateName = \'' + self.AggregateName + '\' and CorpID = \'' + self.CorpID + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages 

class ProductionAdjustments:
    def __init__(self, DBObj, LEName=[], CorpID=[], Date_Key = []):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m
        import pandas as pd

        self.table = 'ProductionAdjustments'
        self.LEName = LEName
        self.CorpID = CorpID
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj
        self.Date_Key = Date_Key

    def ReadTable(self):
        from Model import BPXDatabase as bpx
        import pandas as pd
        from Model import ModelLayer as m
        from datetime import datetime

        Success = True
        Messages = []

        header_df = pd.DataFrame()
        try:
            #Create dictionary to pass to where clause
            where_dict = {}
            if self.LEName:
                where_dict['LEName'] = self.LEName
            if self.CorpID:
                where_dict['CorpID'] = self.CorpID
            if self.Date_Key:
                if isinstance(self.Date_Key[0], datetime):
                    self.Date_Key[0] = self.Date_Key[0].strftime('%Y-%m-%d %H:%M:%S')
                where_dict['Date_Key'] = self.Date_Key

            #Interpret key words as clauses used to filter query
            where_clause = m.ValidateAndClauseArguments(where_dict, self.table, self.DBObj)
            data_df = m.ReadFromTables(self.DBObj, self.table, where_clause)

            rows = []
            for idx, item in data_df.iterrows():
                row = m.ProductionAdjustmentsRow(item['LEName'] , item['WellName'], item['CorpID'], item['Date_Key'], item['AdjustedGasProduction'], item['AdjustedOilProduction'], item['AdjustedWaterProduction'], self.DBObj)
                rows.append(row)

        except Exception as ex:
            rows = []
            Success = False
            Messages.append('Error reading from the database. ' + str(ex))

        return rows, Success, Messages

class ProductionAdjustmentsRow:
    def __init__(self, LEName, WellName, CorpID, Date_Key, AdjustedGasProduction, AdjustedOilProduction, AdjustedWaterProduction,  DBObj):
        from Model import BPXDatabase as bpx
        from Model import ModelLayer as m

        self.LEName = LEName
        self.WellName = WellName
        self.CorpID = CorpID
        self.Date_Key = Date_Key
        self.AdjustedGasProduction = AdjustedGasProduction
        self.AdjustedOilProduction = AdjustedOilProduction
        self.AdjustedWaterProduction = AdjustedWaterProduction
        if not DBObj:
            config = m.GetConfig()
            self.DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        else:
            self.DBObj = DBObj

    def Write(self, Update_User, Update_Date):
        from Model import BPXDatabase as bpx
        from datetime import datetime

        Success = True
        Messages = []

        try:
            if isinstance(Update_Date, datetime):
                Update_Date = Update_Date.strftime('%Y-%m-%d %H:%M:%S')

            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            if not self.AdjustedGasProduction:
                self.AdjustedGasProduction = 0
            if not self.AdjustedOilProduction:
                self.AdjustedOilProduction = 0
            if not self.AdjustedWaterProduction:
                self.AdjustedWaterProduction = 0
            
            insert_statement = 'insert into [LEForecastDatabase].[dbo].[ProductionAdjustments] (LEName, WellName, CorpID, Date_Key, AdjustedGasProduction, AdjustedOilProduction, AdjustedWaterProduction, Update_Date, Update_User)'\
                ' values (\'' +  self.LEName + '\', \'' + self.WellName + '\', \'' + self.CorpID + '\', convert(datetime, \'' +  self.Date_Key + '\', 120) , ' +  str(self.AdjustedGasProduction) + ',\n'\
                '' + str(self.AdjustedOilProduction) + ', ' + str(self.AdjustedWaterProduction) + ',  convert(datetime, \'' + Update_Date + '\', 120), \'' +  Update_User + '\')'

            Success, Message = self.DBObj.Command(insert_statement)
            if Success:
                self.DBObj.Command('commit')
            else:
                Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error writing to the database. ' + str(ex))

        return Success, Messages

    def Update(self, Update_User, Update_Date):
        from Model import ModelLayer as m

        Success = True
        Messages = []

        try:
            #Primary keys for LE Data table, Query the table for existing entry
            ProdAdjustmentsObj = m.ProductionAdjustments(self.DBObj, [self.LEName], [self.CorpID], [self.Date_Key])
            rows, Success, Message = ProdAdjustmentsObj.ReadTable()
            if not Success:
                Messages.append(Message)
            if len(rows) > 1 or not Success:                
                Success = False
                Messages.append('Unsuccessful in attempt to find single entry of data table.')
            elif len(rows) == 0:
                #If no row exists, go ahead and write one
                Success, Message = self.Write(Update_User, Update_Date)
                Messages.append
            else:   
                Success, Message = self.Delete()
                if Success:
                    Success, Message = self.Write(Update_User, Update_Date)
                    Messages.append(Message)
                    if not Success:
                        rows[0].Write(Update_User, Update_Date)
                else:
                    Messages.append(Message)

        except Exception as ex:
            Success = False
            Messages.append('Error updating the Production Adjustments table. ' + str(ex))

        return Success, Messages

    def Delete(self):
        from datetime import datetime
        Success = True
        Messages = []    

        try:
            if isinstance(self.Date_Key, datetime):
                self.Date_Key = self.Date_Key.strftime('%Y-%m-%d %H:%M:%S')

            delete_stmt = 'delete from [LEForecastDatabase].[dbo].[ProductionAdjustments] where LEName = \'' + self.LEName + '\' and CorpID = \'' + self.CorpID + '\' and Date_Key = \'' + self.Date_Key + '\''
            Success, Message = self.DBObj.Command(delete_stmt)
            if not Success:
                Messages.append(Message)
        except Exception as ex:
            Success = False
            Messages.append('Error during delete operation. ' + str(ex))

        return Success, Messages      
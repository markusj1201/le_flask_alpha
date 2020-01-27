import sys
sys.path.append('../')

def WriteAriesScenarioToDB(scenarioName, ForecastName, ForecastYear, start_date, end_date, User, Area, GFO = False, CorpID = ['ALL']):
    from Model import ImportUtility as i
    from Model import BPXDatabase as bpxdb
    from Model import ModelLayer as m
    import datetime as dt

    Success = True
    Messages = []

    try:
        #Query the Aries database using import methods
        scenario_results, Success, Messages = i.ImportAriesByScenario(scenarioName, start_date, end_date, Area)

        #Create NF columns for oil and gas (replace nan with 0)
        scenario_results['OilNF'] = scenario_results['C754'] / scenario_results['GasProduction']
        scenario_results['GasNF'] = scenario_results['C753'] / scenario_results['OilProduction']

        scenario_results = scenario_results.fillna(0)

        #Obtain list from scenario query results
        CorpID_list = scenario_results['CorpID'].to_list()
        CorpID_list = list(set(CorpID_list))

        config = m.GetConfig()
        DBObj = bpxdb.BPXDatabase(config['server'], config['database'], config['UID'])

        #Linearly regress the data
        #Two segments: previous month's mid average and next month's mid average - regress to both to get the values.
        count = 1
        for corpID in CorpID_list:
            #Get the subset of results that match this wellflac
            corpid_scenario_df = scenario_results.query('CorpID == @corpID')
            corpid_scenario_df = corpid_scenario_df.sort_values(by = ['Date'], ascending = True)            

            if corpid_scenario_df.shape[0] > 1:
                df_previous_row = (0, corpid_scenario_df.iloc[1])
                
                wellflac_count = 1
                header_corpID = ''
                for df_row in corpid_scenario_df.iterrows():

                    if wellflac_count == 1:
                        df_next_row = corpid_scenario_df.iloc[wellflac_count]
                        results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], NextMonthVal = df_next_row)                      
                    else:
                        results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], PreviousMonthVal = df_previous_row[1])                      
                                                                
                    Success, Message = WriteInterpolatedForecastToDB(df_row[1]['WellName'], corpID, ForecastName, ForecastYear, scenarioName, GFO, User, results)   

                    if not Success:
                        Messages.append(Message)
                        break

                    df_previous_row = df_row

                    wellflac_count = wellflac_count + 1

            callprogressbar(count, len(CorpID_list))
            count = count + 1

    except Exception as ex:
        Success = False
        Messages.append('Failed to write the results from chosen scenario in Aries database. ' + str(ex))

    return Success, Messages

def SOHA_WriteGFOToDB_2019Database(ForecastName, ForecastYear, User, start_date, end_date, WellFlac = ['ALL'], GFO = False):
    #Part of to be deprecated methods to convert SoHa internal GFO data to standard
    from Model import BPXDatabase as bpxdb
    from Model import QueryFile as qf
    from Model import ImportUtility as imp
    from Model import ModelLayer as m
    import datetime as dt
    import numpy as np

    Sucess = True
    Messages = []

    try:
        config = m.GetConfig()
        #Create DB Object
        return_df, Success, Message = imp.ImportGFOFromDB2019(start_date, end_date, WellFlac)
        if not Success:
            Messages.append(Message)
        Production_Column_Name = '2019Zmcfd'
        Success, Message = WriteInternalForecasttoDB(return_df, ForecastName, ForecastYear, Production_Column_Name,  User, GFO)
        if not Success:
            Messages.append(Message)
        
    except Exception as ex:
        Success = False
        Messages.append('Error writing GFO to DB. ' + str(ex))    
    
    return Success, Messages

def SOHA_WriteGFOToDB_2018Database(ForecastName, ForecastYear, User, start_date, end_date, WellFlac = ['ALL'], GFO = False):
    #Part of to be deprecated methods to convert SoHa internal GFO data to standard
    from Model import BPXDatabase as bpxdb
    from Model import QueryFile as qf
    from Model import ImportUtility as imp
    from Model import ModelLayer as m
    import datetime as dt
    import numpy as np

    Sucess = True
    Messages = []

    try:
        config = m.GetConfig()
        #Create DB Object
        return_df, Success, Message = imp.ImportGFOFromDB2019(start_date, end_date, WellFlac)
        if not Success:
            Messages.append(Message)
        Production_Column_Name = '2018Zmcfd'
        Success, Message = WriteInternalForecasttoDB(return_df, ForecastName, ForecastYear, Production_Column_Name,  User, GFO)
        if not Success:
            Messages.append(Message)
        
    except Exception as ex:
        Success = False
        Messages.append('Error writing GFO to DB. ' + str(ex))    
    
    return Success, Messages

def SOHA_WriteInternalForecasttoDB(df,ForecastName, ForecastYear, Production_Column_Name, User, GFO=True):
    #Part of to be deprecated methods to convert SoHa internal GFO data to standard
    from Model import BPXDatabase as bpx
    from Model import ModelLayer as m
    import datetime as dt
    from Model import QueryFile as qf

    Success = True
    Messages = []

    try:
        config = m.GetConfig()
        DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        EDWObj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')

        wellname_list = df['WellName'].unique()
        wellname_list = list(wellname_list)
        if '' in wellname_list:
            wellname_list.remove('')
        count = 1
        for name in wellname_list:
            monthly_df = df.query('WellName == @name')
            monthly_df = monthly_df.sort_values(by = ['Date'], ascending = True)
            df_previous_row = (0, monthly_df.iloc[1])
            nettingFactor = monthly_df['NettingFactor'].values[0]
            well_count = 1
            header_corpid = ''
            for df_row in monthly_df.iterrows():
                if well_count == 1:
                    df_next_row = monthly_df.iloc[well_count]
                    results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row[1], NextMonthVal = df_next_row, GasRateField=Production_Column_Name)  
                elif well_count != monthly_df.shape[0] and well_count != 1:
                    df_next_row = monthly_df.iloc[well_count]
                    results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row[1], NextMonthVal = df_next_row, PreviousMonthVal = df_previous_row[1], GasRateField=Production_Column_Name)  
                elif well_count == monthly_df.shape[0]:
                    results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row[1], PreviousMonthVal = df_previous_row[1], GasRateField=Production_Column_Name)

                for row in results.iterrows():
                    corpid_query = qf.EDWKeyQueryFromWellName([name])            
                    corpid_results = EDWObj.Query(corpid_query)
                    if not corpid_results[1].empty:
                        CorpID = corpid_results[1].at[0,'CorpID']
                    else:
                        CorpID = name
                    WellName = name
                    Update_Date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")            
                    Update_User = User
                    if header_corpid != CorpID:
                        #Create Header entry    
                        header_corpid = CorpID
                        ForecastHeaderObj = m.ForecastHeaderRow(WellName, CorpID, ForecastName, ForecastYear, '', [], GFO, DBObj)                                                                          
                        Success, Message = ForecastHeaderObj.Write(Update_User, Update_Date)
                        if not Success:
                            Messages.append(Message)

                    Date_Key = row[1]['Date'].strftime('%m/%d/%Y')
                    Gas_Production = row[1]['GasProduction']
                    GasNF = row[1]['GasNF']
                    if Gas_Production >= 0 and Date_Key:
                        ForecastDataObj = m.ForecastDataRow(ForecastName, CorpID, Date_Key, Gas_Production, 0, 0, GasNF, 0, 0, DBObj)     
                        Success, Message = ForecastDataObj.Write(Update_User, Update_Date)
                        if not Success:
                            Messages.append(Message)

                    df_previous_row = df_row

                well_count = well_count + 1
                            
            callprogressbar(count, len(wellname_list))
            count = count + 1
        

    except Exception as ex:
        Success = False
        Messages.append('Error writing Forecast to Database. ' + str(ex))

    return Success, Messages

def SOHA_WriteGasNettingFactorsFromDB(Update_User, Update_Date, wellnames = []):
    from Model import BPXDatabase as bpx
    from Model import QueryFile as qf
    from Model import ModelLayer as m
    import datetime as datetime

    Success = True
    Messages = []

    try:
        config = m.GetConfig()
        DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])
        TeamOpsObj = bpx.GetDBEnvironment('OnPrem', 'OVERRIDE')
        EDWObj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')

        #Get Well List of required netting values from data that is already in database.
        query = qf.GetNettingFactorsfromDB(wellnames)
        res, res_df = TeamOpsObj.Query(query)

        count = 1
        for idx, item in res_df.iterrows():
            wellquery = qf.EDWKeyQueryFromWellName([item['WellName']])
            res, well_row = EDWObj.Query(wellquery)
            if not well_row.empty:
                corpID = well_row['CorpID'].values[0]
                NettingObj = m.GasNettingRow(item['WellName'], corpID, item['NF'], item['FirstSalesDateInput'], DBObj)
                Success, Message = NettingObj.Write(Update_User, Update_Date)
                if not Success:
                    Messages.append(Message)

            callprogressbar(count, res_df.shape[0])
            count = count + 1

    except Exception as ex:
        Success = False
        Messages.append('Error during write of netting factors to DB. ' + str(ex))

    return Success, Messages

def WriteDefaultMultipliers(LE_Name, DefaultValue, Update_User, Update_Date, SuppressMessages):
    import datetime as datetime
    from Model import BPXDatabase as bpx
    from Model import ModelLayer as m

    Success = True
    Messages = []

    try:        
        config = m.GetConfig()
        DBObj = bpx.BPXDatabase(config['server'], config['database'], config['UID'])

        #Query the LE results 
        LE_query = 'select * from [LEForecastDatabase].[dbo].[LE_Data] where HeaderName = \'' + LE_Name + '\''
        res, df = DBObj.Query(LE_query)

        count = 1
        for idx, row in df.iterrows():
            FracHitObj = m.FracHitMultipliersRow(row['HeaderName'], row['CorpID'], row['Date_Key'], str(DefaultValue), DBObj)
            Success, Message = FracHitObj.Write(Update_User, Update_Date)
            if not Success:
                Messages.append(Message)

            if not SuppressMessages:
                callprogressbar(count, df.shape[0])
            count = count + 1

    except Exception as ex:
        Success = False
        Messages.append('Error during write of default frac hit multipliers. ' + str(ex))

    return Success, Messages

def WriteLEFromExcel(LEName, LE_Date,filename, sheetname, IDstartrow, corpID_col, wellName_col, date_row, date_startcol, date_endcol, InterpolationMethod, Phase, Update_User, Update_Date, IDs = ['ALL'] ):
    from datetime import datetime, date
    import pandas as pd
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from Model import ImportUtility as i

    Messages = []
    Success = True
    try:
        all_data_df, Success, Message = i.ImportForecastFromExcel(filename, sheetname, IDstartrow, corpID_col, wellName_col, date_row, date_startcol, date_endcol, Phase, '', '', ['ALL'])
        if Success:
            if corpID_col:
                IDCol = 'CorpID'
            else:
                IDCol = 'WellName'
            Success, Message = WriteLEFromTemplate(all_data_df, InterpolationMethod, LEName, LE_Date, Update_User, IDCol)
            if not Success:
                Messages.append(Message)
        else:
            Messages.append(Message)

    except Exception as ex:
        Success = False
        Messages.append('Error during write of LE data from Excel sheet. ' + str(ex))

    return Success, Messages

def WriteForecastFromExcel(ForecastName, ForecastYear,scenarioName, GFO, filename, sheetname, IDstartrow, corpID_col, wellName_col, date_row, date_startcol, date_endcol, InterpolationMethod, Phase, Update_User, Update_Date, IDs = ['ALL'] ):
    from datetime import datetime, date
    import pandas as pd
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from Model import ImportUtility as i

    Messages = []
    Success = True
    try:
        all_data_df, Success, Message = i.ImportForecastFromExcel(filename, sheetname, IDstartrow, corpID_col, wellName_col, date_row, date_startcol, date_endcol, Phase, '', '', ['ALL'])
        if Success:
            if corpID_col:
                IDCol = 'CorpID'
            else:
                IDCol = 'WellName'

            Success, Message = WriteForecastFromTemplate(all_data_df, InterpolationMethod, ForecastName, ForecastYear, scenarioName, GFO, Update_User, IDCol)

            if not Success:
                Messages.append(Message)
        else:
            Messages.append(Message)

        if not Success:
            Messages.append(Message)

    except Exception as ex:
        Success = False
        Messages.append('Error during the write of Forecast from Excel sheet. ' + str(ex))

    return Success, Messages

def WriteForecastFromTemplate(all_data_df, InterpolationMethod, ForecastName, ForecastYear, scenarioName, GFO, Update_User, IDCol='WellName'):
    from datetime import datetime, date
    import pandas as pd
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from Model import ImportUtility as i

    Success = True
    Messages = []
    results = []
    try:
        #Data Frame must be the same structure as the output from the 'Read From Excel Function
        #'CorpID', 'WellName', 'Wedge', 'Date', 'Gas', 'Oil', 'Water', 'OilNF', 'GasNF'

        wellname = ''
        if not Success:
            Messages.append(Message)

        if IDCol == 'CorpID':
            corpid_list = list(all_data_df['CorpID'].unique())
            corpid_query = qf.EDWKeyQueryFromCorpID(corpid_list)
            corpid_results, corpid_df = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE').Query(corpid_query)
            well_list = list(corpid_df['WellName'].unique())

            well_query = 'CorpID == @corpid'
        else:
            well_list = list(all_data_df['WellName'].unique())
            well_query = 'WellName == @wellname'

        well_list = [i for i in well_list if i] 

        for wellname in well_list:            
            wellname, corpid = i.GetWellandCorpID(wellname, '')
            if not corpid:
                corpid = wellname
             
            data_df = all_data_df.query(well_query)

            row_count = 1
            if not data_df.empty:
                df_previous_row = (0, data_df.iloc[1])            

                for idx, df_row in data_df.iterrows():
                    if InterpolationMethod == 'MonthlyRates':
                        if row_count == 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, NextMonthVal = df_next_row, GasProduction='Gas', OilProduction='Oil')              

                        elif row_count != data_df.shape[0] and row_count != 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, NextMonthVal = df_next_row, PreviousMonthVal = df_previous_row, GasProduction='Gas', OilProduction='Oil')                  

                        elif row_count == data_df.shape[0]:
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, PreviousMonthVal = df_previous_row, GasProduction='Gas', OilProduction='Oil')

                    elif InterpolationMethod == 'MonthlyVolume':
                        if row_count == 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], NextMonthVal = df_next_row)                      
                        else:
                            results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], PreviousMonthVal = df_previous_row[1])   

                    elif InterpolationMethod == 'None':
                        results = ConvertNonInterpolatedResults(df_row)                      

                    Success, Message = WriteInterpolatedForecastToDB(wellname, corpid, ForecastName, ForecastYear, scenarioName, GFO, Update_User, results)
                    if not Success:
                        Messages.append(Message)
                        
                    df_previous_row = df_row
                    row_count = row_count + 1

    except Exception as ex:
        Success = False
        Messages.append('Error during the writing of the forecast from template. ' + str(ex))

    return Success, Messages

def WriteLEFromTemplate(all_data_df, InterpolationMethod,  LEName, LE_Date, Update_User, IDCol = 'WellName'):
    from datetime import datetime, date
    import pandas as pd
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from Model import ImportUtility as i

    Success = True
    Messages = []
    results = []
    try:
        #Data Frame must be the same structure as the output from the 'Read From Excel Function
        #'CorpID', 'WellName', 'Wedge', 'Date', 'Gas', 'Oil', 'Water', 'OilNF', 'GasNF'

        wellname = ''
        if not Success:
            Messages.append(Message)

        if IDCol == 'CorpID':
            corpid_list = list(all_data_df['CorpID'].unique())
            corpid_query = qf.EDWKeyQueryFromCorpID(corpid_list)
            corpid_results, corpid_df = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE').Query(corpid_query)
            well_list = list(corpid_df['WellName'].unique())

            well_query = 'CorpID == @corpid'
        else:
            well_list = list(all_data_df['WellName'].unique())
            well_query = 'WellName == @wellname'


        well_list = [i for i in well_list if i] 

        for wellname in well_list:             

            wellname, corpid = i.GetWellandCorpID(wellname, '')
            if not corpid:
                corpid = wellname
                
            data_df = all_data_df.query(well_query)

            row_count = 1
            if not data_df.empty:
                df_previous_row = (0, data_df.iloc[1])

                for idx, df_row in data_df.iterrows():
                    if InterpolationMethod == 'MonthlyRates':
                        if row_count == 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, NextMonthVal = df_next_row, GasProduction='Gas', OilProduction='Oil')              

                        elif row_count != data_df.shape[0] and row_count != 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, NextMonthVal = df_next_row, PreviousMonthVal = df_previous_row, GasProduction='Gas', OilProduction='Oil')                  

                        elif row_count == data_df.shape[0]:
                            results = InterpolateDailyRatesFromMonthlyRates(CurrentMonthVal = df_row, PreviousMonthVal = df_previous_row, GasProduction='Gas', OilProduction='Oil')

                    elif InterpolationMethod == 'MonthlyVolume':
                        if row_count == 1:
                            df_next_row = data_df.iloc[row_count]
                            results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], NextMonthVal = df_next_row)                      
                        else:
                            results = InterpolateDailyRatesFromMonthlyVolumes(CurrentMonthVal = df_row[1], PreviousMonthVal = df_previous_row[1])     

                    elif InterpolationMethod == 'None':
                        results = ConvertNonInterpolatedResults(df_row)

                    Wedge, Message = i.GetWedgeData(corpid, True)
                    Success, Message = WriteInterpolatedLEToDB(LEName, wellname, corpid, '', Wedge, LE_Date, Update_User, results)
                    if not Success:
                        Messages.append(Message)
                        
                    df_previous_row = df_row
                    row_count = row_count + 1

    except Exception as ex:
        Success = False
        Messages.append('Error during the writing of the LE from template. ' + str(ex))

    return Success, Messages

def WriteInterpolatedForecastToDB(WellName, corpID, ForecastName, ForecastYear, scenarioName, GFO, UserName, results):
    import datetime as dt
    import pandas as pd
    from Model import ModelLayer as m

    header_corpID = ''
    Messages = []
    for item in results.iterrows():      
        idx = item[0]                 
        UpdateDate = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        
        if header_corpID != corpID:   
            ForecastHeaderObj = m.ForecastHeaderRow(WellName, corpID, ForecastName, ForecastYear, scenarioName, [], GFO, '')                                               
            Success, Message = ForecastHeaderObj.Write(UserName, UpdateDate)
            if not Success:
                Messages.append(Message)
            else:
                header_corpID = corpID
                            
        Date_Key = item[1]['Date'].strftime('%m/%d/%Y')
        Gas_Production = item[1]['GasProduction']   
        Oil_Production = item[1]['OilProduction']
        GasNF = item[1]['GasNF']         
        OilNF = item[1]['OilNF']
        ForecastDataObj = m.ForecastDataRow(ForecastName, corpID, Date_Key, Gas_Production, Oil_Production, 0, GasNF, OilNF, 0, '')
        Success, Message = ForecastDataObj.Write(UserName, UpdateDate)
        if not Success:
            Messages.append(Message)    

    return Success, Messages

def WriteInterpolatedLEToDB(LEName, WellName, CorpID, ForecastGeneratedFrom, Wedge, LE_Date, UserName, results):
    import datetime as dt
    import pandas as pd
    from Model import ModelLayer as m

    header_corpID = ''
    Messages = []
    for item in results.iterrows():      
        idx = item[0]                 
        UpdateDate = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        
        if header_corpID != CorpID:   
            LEHeaderObj = m.LEHeaderRow(LEName, WellName, CorpID, ForecastGeneratedFrom, Wedge, LE_Date, '')
            Success, Message = LEHeaderObj.Write(UserName, UpdateDate)
            if not Success:
                Messages.append(Message)
            else:
                header_corpID = CorpID
                            
        Date_Key = item[1]['Date'].strftime('%m/%d/%Y')
        Gas_Production = item[1]['GasProduction']   
        Oil_Production = item[1]['OilProduction']
        LEDataObj = m.LEDataRow(LEName, CorpID, Date_Key, Gas_Production, Oil_Production, 0, '')
        Success, Message = LEDataObj.Write(UserName, UpdateDate)
        if not Success:
            Messages.append(Message)    

    return Success, Messages

def callprogressbar(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█'):

    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = '\r')
    # Print New Line on Complete
    if iteration == total: 
        print()

def InterpolateDailyRatesFromMonthlyVolumes(**kwargs):
    #Take in the monthly cumulative volumes that are assinged at the 'end' of the month in Aries
    #Assign daily production and return the results
    from datetime import timedelta
    import pandas as pd
    import math

    previous_month_val = ''
    current_month_val = ''
    next_month_val = ''
    return_df = pd.DataFrame(columns = ['GasProduction', 'OilProduction', 'GasNF', 'OilNF', 'Date'])

    previous_month_bool = False
    current_month_bool = False
    next_month_bool = False

    for key, value in kwargs.items():
        if key=='PreviousMonthVal':
            previous_month_val = value
            previous_month_bool = True
        elif key=='CurrentMonthVal':
            current_month_val = value
            current_month_bool = True
        elif key == 'NextMonthVal':
            next_month_val = value
            next_month_bool = True

    if previous_month_bool and current_month_bool and not next_month_bool:
        #Get number of days between previous val and current val (should be roughly 1 month)
        previous_date = previous_month_val['Date']
        current_month_date = current_month_val['Date']
        diff = current_month_date - previous_date
        days = diff.days

        normal_days = 30.42

        #Get slope between the two values
        previous_gas_volume = previous_month_val['GasProduction']
        current_gas_volume = current_month_val['GasProduction']
        gas_slope = (current_gas_volume - previous_gas_volume) / normal_days #Average days in a month

        previous_oil_volume = previous_month_val['OilProduction']
        current_oil_volume = current_month_val['OilProduction']
        oil_slope = (current_oil_volume - previous_oil_volume) / normal_days

        if current_gas_volume > 0:
            gasnettingFactor = current_month_val['GasNF']
        else:
            gasnettingFactor = 0

        if current_oil_volume > 0:
            oilnettingFactor = current_month_val['OilNF']       
        else:
            oilnettingFactor = 0
        
        return_row = {}
        for day in range(days):
            #Add an entry to the return data frame
            gas_production = previous_gas_volume + (day + 1) * gas_slope
            if gas_production > 0:
                return_row['GasProduction'] = gas_production / normal_days
            else:
                return_row['GasProduction'] = 0

            oil_production = previous_oil_volume + (day + 1) * oil_slope
            if gas_production > 0:
                return_row['OilProduction'] = oil_production / normal_days
            else:
                return_row['OilProduction'] = 0

            return_row['Date'] = previous_date + timedelta(days = (day+1)) 
            return_row['GasNF'] = gasnettingFactor
            return_row['OilNF'] = oilnettingFactor
            return_df = return_df.append(return_row, ignore_index = True)


    elif current_month_bool and next_month_bool and not previous_month_bool:
        current_month_date = current_month_val['Date']
        next_month_date = next_month_val['Date']
        diff = next_month_date - current_month_date
        days =current_month_date.day
    
        normal_days = 30.42

        current_gas_volume = current_month_val['GasProduction']
        next_gas_volume = next_month_val['GasProduction']
        gas_slope = (next_gas_volume - current_gas_volume) / normal_days

        current_oil_volume = current_month_val['OilProduction']
        next_oil_volume = next_month_val['OilProduction']
        oil_slope = (next_oil_volume - current_oil_volume) / normal_days

        if current_gas_volume > 0:
            gasnettingFactor = current_month_val['GasNF']          
        else:
            gasnettingFactor = 0        

        if current_oil_volume > 0:
            oilnettingFactor = current_month_val['OilNF']             
        else:
            oilnettingFactor = 0        

        return_row = {}
        for day in range(days):
            gas_production = current_gas_volume - day * gas_slope
            if gas_production > 0:
                return_row['GasProduction'] = gas_production / normal_days
            else:
                return_row['GasProduction'] = 0

            oil_production = current_oil_volume - day * oil_slope
            if oil_production > 0:
                return_row['OilProduction'] = oil_production / normal_days
            else:
                return_row['OilProduction'] = 0

            return_row['Date'] = current_month_date - timedelta(days = day)
            return_row['GasNF'] = gasnettingFactor
            return_row['OilNF'] = oilnettingFactor
            return_df = return_df.append(return_row, ignore_index = True)
    
    return return_df

def ConvertNonInterpolatedResults(df_row):
    from datetime import datetime, timedelta
    import pandas as pd

    return_df = pd.DataFrame(columns = ['GasProduction', 'OilProduction', 'GasNF', 'OilNF', 'Date'])

    return_row = {}
    return_row['GasProduction'] = df_row['Gas']
    return_row['OilProduction'] = df_row['Oil']
    return_row['GasNF'] = df_row['GasNF']
    return_row['OilNF'] = df_row['OilNF']
    return_row['Date'] = df_row['Date']
    return_df = return_df.append(return_row, ignore_index = True)

    return return_df

def InterpolateDailyRatesFromMonthlyRates(**kwargs):
    #Take in the monthly average rates that are assinged at the 'beginning' of the month in internal databases
    #Assign daily production and return the results
    from datetime import datetime, timedelta
    import pandas as pd
    import math

    previous_month_val = ''
    current_month_val = ''
    next_month_val = ''
    return_df = pd.DataFrame(columns = ['GasProduction', 'OilProduction', 'GasNF', 'OilNF', 'Date'])

    previous_month_bool = False
    current_month_bool = False
    next_month_bool = False

    for key, value in kwargs.items():
        if key=='PreviousMonthVal':
            previous_month_val = value
            previous_month_bool = True
        elif key=='CurrentMonthVal':
            current_month_val = value
            current_month_bool = True
        elif key == 'NextMonthVal':
            next_month_val = value
            next_month_bool = True
        elif key == 'GasProduction':
            gas_rate = value
        elif key == 'OilProduction':
            oil_rate = value

    if previous_month_bool and current_month_bool and not next_month_bool:
        #Scenario for the end of the analysis when no next month's data exists
        current_month_date = current_month_val['Date']
        previous_date = previous_month_val['Date']

        current_gas_rate = current_month_val[gas_rate]
        previous_gas_rate = previous_month_val[gas_rate]

        current_oil_rate = current_month_val[oil_rate]
        previous_oil_rate = previous_month_val[oil_rate]

        days_in_month = pd.to_datetime(current_month_date + pd.tseries.offsets.MonthEnd(1)).date().day
        days_in_previous = pd.to_datetime(previous_date + pd.tseries.offsets.MonthEnd(1)).date().day
        mid_current = datetime(year = current_month_date.year, month = current_month_date.month, day= math.floor(days_in_month / 2)) 
        mid_previous = datetime(year = previous_date.year, month = previous_date.month, day= math.floor(days_in_previous / 2)) 

        backward_days = (mid_current - mid_previous).days
        gas_backward_slope = (current_gas_rate - previous_gas_rate) / backward_days
        oil_backward_slope = (current_oil_rate - previous_oil_rate) / backward_days

        return_row = {}
        for day in range(days_in_month):
            if day < mid_current.day:
                applied_days = mid_current.day - day
                gas_production = current_gas_rate - applied_days * gas_backward_slope
                oil_production = current_oil_rate - applied_days * oil_backward_slope
            elif day == mid_current.day:
                gas_production = current_gas_rate
                oil_production = current_oil_rate
            elif day > mid_current.day:
                applied_days = day - mid_current.day
                gas_production = current_gas_rate + applied_days * gas_backward_slope
                oil_production = current_oil_rate + applied_days * oil_backward_slope

            if gas_production > 0:
                return_row['GasProduction'] = gas_production 
            else:
                return_row['GasProduction'] = 0

            if oil_production > 0:
                return_row['OilProduction'] = oil_production 
            else:
                return_row['OilProduction'] = 0

            return_row['Date'] = current_month_date + timedelta(days = day)
            return_row['GasNF'] = current_month_val['GasNF']
            return_row['OilNF'] = current_month_val['OilNF']
            return_df = return_df.append(return_row, ignore_index = True)        


    elif current_month_bool and next_month_bool and not previous_month_bool:
        #Scenario for the beginning of the analysis when no previous month's data exists
        current_month_date = current_month_val['Date']
        next_month_date = next_month_val['Date']

        current_gas_rate = current_month_val[gas_rate]
        next_gas_rate = next_month_val[gas_rate]

        current_oil_rate = current_month_val[oil_rate]
        next_oil_rate = next_month_val[oil_rate]

        days_in_month = pd.to_datetime(current_month_date + pd.tseries.offsets.MonthEnd(1)).date().day        
        days_in_next = pd.to_datetime(next_month_date + pd.tseries.offsets.MonthEnd(1)).date().day
        mid_current = datetime(year = current_month_date.year, month = current_month_date.month, day= math.floor(days_in_month / 2)) 
        mid_next = datetime(year = next_month_date.year, month = next_month_date.month, day= math.floor(days_in_next / 2)) 

        forward_days = (mid_next - mid_current).days
        gas_forward_slope = (next_gas_rate - current_gas_rate) / forward_days
        oil_forward_slope = (next_oil_rate - current_oil_rate) / forward_days

        return_row = {}
        for day in range(days_in_month):
            if day < mid_current.day:
                applied_days = mid_current.day - day
                gas_production = current_gas_rate - applied_days * gas_forward_slope
                oil_production = current_oil_rate - applied_days * oil_forward_slope
            elif day == mid_current.day:
                gas_production = current_gas_rate
                oil_production = current_oil_rate
            elif day > mid_current.day:
                applied_days = day - mid_current.day
                gas_production = current_gas_rate + applied_days * gas_forward_slope
                oil_production = current_oil_rate + applied_days * oil_forward_slope

            if gas_production > 0:
                return_row['GasProduction'] = gas_production 
            else:
                return_row['GasProduction'] = 0

            if oil_production > 0:
                return_row['OilProduction'] = oil_production 
            else:
                return_row['OilProduction'] = 0

            return_row['Date'] = current_month_date + timedelta(days = day)
            return_row['GasNF'] = current_month_val['GasNF']
            return_row['OilNF'] = current_month_val['OilNF']
            return_df = return_df.append(return_row, ignore_index = True)        

    elif current_month_bool and next_month_bool and previous_month_bool:
        #Most common scenario - compare the slope between the current and previous to backfill dates 
        #Compare the current to next to forward fill dates
        current_month_date = current_month_val['Date']
        next_month_date = next_month_val['Date']
        previous_date = previous_month_val['Date']

        gas_current_rate = current_month_val[gas_rate]
        gas_next_rate = next_month_val[gas_rate]
        gas_previous_rate = previous_month_val[gas_rate]

        oil_current_rate = current_month_val[oil_rate]
        oil_next_rate = next_month_val[oil_rate]
        oil_previous_rate = previous_month_val[oil_rate]        

        days_in_month = pd.to_datetime(current_month_date + pd.tseries.offsets.MonthEnd(1)).date().day
        days_in_previous = pd.to_datetime(previous_date + pd.tseries.offsets.MonthEnd(1)).date().day
        days_in_next = pd.to_datetime(next_month_date + pd.tseries.offsets.MonthEnd(1)).date().day
        mid_current = datetime(year = current_month_date.year, month = current_month_date.month, day= math.floor(days_in_month / 2)) 
        mid_previous = datetime(year = previous_date.year, month = previous_date.month, day= math.floor(days_in_previous / 2)) 
        mid_next = datetime(year = next_month_date.year, month = next_month_date.month, day= math.floor(days_in_next / 2)) 

        forward_days = (mid_next - mid_current).days
        backward_days = (mid_current - mid_previous).days

        gas_backward_slope = (gas_current_rate - gas_previous_rate) / backward_days
        gas_forward_slope = (gas_next_rate - gas_current_rate) / forward_days

        oil_backward_slope = (oil_current_rate - oil_previous_rate) / backward_days
        oil_forward_slope = (oil_next_rate - oil_current_rate) / forward_days

        return_row = {}
        for day in range(days_in_month):
            if day < mid_current.day:
                applied_days = mid_current.day - day
                gas_production = gas_current_rate - applied_days * gas_backward_slope
                oil_production = oil_current_rate - applied_days * oil_backward_slope
            elif day == mid_current.day:
                gas_production = gas_current_rate
                oil_production = oil_current_rate
            elif day > mid_current.day:
                applied_days = day - mid_current.day
                gas_production = gas_current_rate + applied_days * gas_forward_slope
                oil_production = oil_current_rate + applied_days * oil_forward_slope

            if gas_production > 0:
                return_row['GasProduction'] = gas_production 
            else:
                return_row['GasProduction'] = 0

            if oil_production > 0:
                return_row['OilProduction'] = oil_production 
            else:
                return_row['OilProduction'] = 0

            return_row['Date'] = current_month_date + timedelta(days = day)
            return_row['GasNF'] = current_month_val['GasNF']
            return_row['OilNF'] = current_month_val['OilNF']
            return_df = return_df.append(return_row, ignore_index = True)        

    return return_df

def CreateAreaWellsFromRoute(new_route_name, db_route_name, Update_User):
    from Model import ModelLayer as m
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from datetime import datetime
    import pandas as pd

    Success = True
    Messages = []

    try:

        ODSObj = bpx.GetDBEnvironment('ProdODS', 'OVERRIDE')
        EDWObj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')
        query = qf.RouteQuery([db_route_name])
        results = ODSObj.Query(query)

        for idx, row in results[1].iterrows():
            wellflac_query = qf.EDWKeyQueryFromWellFlac([row['wellflac']])
            corpIDres = EDWObj.Query(wellflac_query)
            if not corpIDres[1].empty:
                corpID = corpIDres[1]['CorpID'].iloc[0]
                AggregateRowObj = m.AreaAggregationRow(new_route_name, row['name'], corpID, '')
                Row_Success, Message = AggregateRowObj.Write(Update_User, datetime.now())

                if not Row_Success:
                    Messages.append(Message)
            else:
                Messages.append('Missing well entry in key database EDW: ' + row['name'])

    except Exception as ex:
        Success = False
        Messages.append('Error during Area Aggegation interface from Enbase. ' + str(ex))

    return Success, Messages

def CreateAreaFromWells(new_route_name, well_list, Update_User):
    from Model import ModelLayer as m
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    from datetime import datetime
    import pandas as pd

    Success = True
    Messages = []

    try:
        WellQuery = qf.EDWKeyQueryFromWellName(well_list)
        obj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')

        wells, wells_df  = obj.Query(WellQuery)
        for idx,  well in wells_df.iterrows():
            WellName = well['WellName']
            corpID = well['CorpID']
            AggregateRowObj = m.AreaAggregationRow(new_route_name, WellName, corpID, '')
            Row_Success, Message = AggregateRowObj.Write(Update_User, datetime.now())

            if not Row_Success:
                Messages.append(Message)

    except Exception as ex:
        Success = False
        Messages.append('Error during the creation of the area from well list. ' + str(ex))

    return Success, Messages
    
if __name__ == '__main__':
    from datetime import datetime

    #Test Imports
    # Success, Messages = WriteGFOToDB_2019Database('Travis_Internal_GFOForecast', 2019, 'Travis Comer', '01/01/2019', '12/31/2019', ['ALL'], True)
    # Success, Messages = WriteAriesScenarioToDB('JA_HV', 'Aries_GFOz', '2019', '01/01/2018', '12/31/2021', 'Travis Comer', 'SoHa', GFO = True, CorpID = ['ALL'])

    #Test Reads
    # from Model import ModelLayer as m
    # ForecastHeaderObj = m.ForecastHeader('', ['Blocker'], [], [])
    # rows, Success, Message = ForecastHeaderObj.ReadTable()

    # rows[0].b = 1
    # rows[0].Update('TC', datetime.now())

    # ForecastDataObj = ForecastData('', ['Test_Travis_Forecast'], [])
    # rows, Success, Message = ForecastDataObj.ReadTable()

    # rows[0].Oil_Production = 33
    # Success, Messages = rows[0].Update('TC', datetime.now())

    #Test Writing LE From Excel
    # Success, Messages = WriteLEFromExcel('Test_Excel_LE', r"C:\Users\travis.comer\OneDrive - BP - L48\Documents\Work\Daily Production Forecasts\LE Examples\LE Summary (2).xlsx", 'Daily LE Gross', 18, '', 1, 4, 6, 36, 'Travis Comer', datetime.now(), ['ALL'])

    #Test obtaining data for NF
    # Success, Messages = WriteNettingFactorsFromDB('Travis Comer', datetime.now(), [])

    #Test writing the multiplier defaults
    # Success, Messages = WriteDefaultMultipliers('Test_Create', 1, 'Travis Comer', datetime.now())

    #Test Creation of Routes from Enbase into system
    # Success, Messages = CreateAreaWellsFromRoute('Nash', 'Travis Comer')

    
    print(Success)
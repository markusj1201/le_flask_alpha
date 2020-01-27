"""
***************************************************************************************
   Description: This module is designed to compile and return relevant infomrmation to 
   a front end GUI for graphing and visual purposes
   ***********************************************************************************
   Input Parameters:   | N/A
   Output Parameters:  | N/A
   Tables Accessed:    | All of LE Schema
   Tables Affected:    | N/A
   ----------------------------------------------------------------------------------
                                  Version Control
   ----------------------------------------------------------------------------------
   Version    Developer   Date       Change
   -------    ---------   ---------- ------------------------------------------------
   1.0        Travis C    08/01/2019 Initial Creation
***************************************************************************************
"""

import sys
sys.path.append('../')


def GetFirstOfMonthLE(Date, WellorArea, Wedge, NameFilter):
    from datetime import datetime, timedelta
    import pandas as pd
    from Model import ModelLayer as m
    import calendar

    #Return all LE names that match the input criteria
    Success = True
    Messages = []
    ret_LE_list = []
    try:
        Date = pd.to_datetime(Date)
        #First of Month LE will be defined as any LE within the first 7 days of the beginning of the month.
        first_of_month = datetime(Date.year, Date.month, 1)
        date_range = calendar.monthrange(Date.year, Date.month)
        end_day = date_range[1]
        end_of_month = datetime(Date.year, Date.month, end_day)
        #Query the database for any LE header entries that match the WellorArea and Wege provided
        
        ret_LE_HeaderRows, Success, Message = SelectLEByCriteria(Wedge, WellorArea, NameFilter, first_of_month, end_of_month) 

    except Exception as ex:
        Messages.append('Error during query of LE information. ' + str(ex))
        Success = False


    return ret_LE_HeaderRows, Success, Messages
    
def GetLastWeekLE(Date, WellorArea, Wedge, NameFilter):
    from datetime import datetime, timedelta
    import pandas as pd
    from Model import ModelLayer as m
    
   #Return all LE names that match the input criteria
    Success = True
    Messages = []
    ret_LE_list = []
    try:
        Date = pd.to_datetime(Date)

        #Find the previous monday and use that as the beginning of the previous week
        last_week_start = Date - timedelta(days = (Date.weekday() + 7))
        last_week_end = last_week_start + timedelta(days = 6)

        ret_LE_HeaderRows, Success, Message = SelectLEByCriteria(Wedge, WellorArea, NameFilter, last_week_start, last_week_end) 

    except Exception as ex:
        Messages.append('Error during query of LE information. ' + str(ex))
        Success = False

    return ret_LE_HeaderRows, Success, Messages

def GetLEProduction(LEName, Wedge, WellorArea, Phase = 'Gas'):
    from datetime import datetime
    from Model import ModelLayer as m
    from Controller import SummaryModule as s
    import pandas as pd
    #Get the production values from the LE table by the input criteria
    Success = True
    Messages = []
    ProductionDataObj = []
    try:

        #Get all CorpIDs from Well or Area passed
        WellList = s.GetFullWellList(WellorArea)
        LERowObj = m.LEData('', [LEName], WellList, [])
        LEData, Success, Message = LERowObj.ReadTable()
        if not Success:
            Messages.append(Message)
        elif len(LEData) > 0:
            LEData = pd.DataFrame([vars(s) for s in LEData])
            if Wedge:
                prod_LE_rows = LEData.query('Wedge == @Wedge')
            else:
                prod_LE_rows = LEData

            dates = prod_LE_rows['Date_Key'].unique()
            prod_array = []
            date_array = []
            if not prod_LE_rows.empty:
                if Phase == 'Gas':
                    for date in dates:
                        results = prod_LE_rows.query('Date_Key == @date')
                        prod_array.append(results['Gas_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'scf')
                elif Phase == 'Water':
                    for date in dates:
                        results = prod_LE_rows.query('Date_Key == @date')
                        prod_array.append(results['Water_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'bbl')
                elif Phase == 'Oil':
                    for date in dates:
                        results = prod_LE_rows.query('Date_Key == @date')
                        prod_array.append(results['Oil_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'bbl')
        else:
            Messages.append('No Production data available with this criteria. ')

    except Exception as ex:
        Success = False
        Messages.append('Error during collection of production data. ' + str(ex))

    return ProductionDataObj, Success, Messages

def GetForecastName(WellorArea, Wedge, NameFilter, GFOz = False):
    import pandas as pd
    from Model import ModelLayer as m
    
    #Return all LE names that match the input criteria
    Success = True
    Messages = []
    ret_forecast_list = []
    try:

        #Query the database for any LE header entries that match the WellorArea and Wege provided
        ret_forecast_list, Success, Message = SelectForecastByCriteria(WellorArea, Wedge, NameFilter, GFOz)

    except Exception as ex:
        Messages.append('Error during query of LE information. ' + str(ex))
        Success = False


    return ret_forecast_list, Success, Messages

def GetForecastProduction(ForecastName, Wedge, WellorArea, Phase = 'Gas'):
    from datetime import datetime
    from Model import ModelLayer as m
    import pandas as pd
    from Model import BPXDatabase as bpx
    from Model import QueryFile as qf
    
    #Get the production values from the LE table by the input criteria
    Success = True
    Messages = []
    ProductionDataObj = []
    try:

        #Get all CorpIDs from Well or Area passed
        well_list = []
        corpid_list = []
        if WellorArea:
            well_list.append(WellorArea)
            EDWobj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')
            corpid_query = qf.EDWKeyQueryFromWellName(well_list)
            corpid_list = EDWobj.Query(corpid_query)
            corpid_list = list(corpid_list[1]['CorpID'])
        if Wedge:
            wedge_list, Success, Message = GetWellorAreaByWedge(Wedge)
            if not Success:
                Messages.append(Message)
            else:
                well_list.extend(wedge_list)

        ForecastDataRowObj = m.ForecastData('', [ForecastName], corpid_list, [])
        ForecastData, Success, Message = ForecastDataRowObj.ReadTable()
        if not Success:
            Messages.append(Message)
        else:
            prod_Forecast_rows = pd.DataFrame([vars(s) for s in ForecastData])

            dates = prod_Forecast_rows['Date_Key'].unique()
            prod_array = []
            date_array = []
            if not prod_Forecast_rows.empty:
                if Phase == 'Gas':
                    for date in dates:
                        results = prod_Forecast_rows.query('Date_Key == @date')
                        prod_array.append(results['Gas_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'scf')
                elif Phase == 'Water':
                    for date in dates:
                        results = prod_Forecast_rows.query('Date_Key == @date')
                        prod_array.append(results['Water_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'bbl')
                elif Phase == 'Oil':
                    for date in dates:
                        results = prod_Forecast_rows.query('Date_Key == @date')
                        prod_array.append(results['Oil_Production'].sum())
                        date_array.append(date)
                    ProductionDataObj = ProductionData(date_array, prod_array, Phase,'bbl')

    except Exception as ex:
        Success = False
        Messages.append('Error during collection of production data. ' + str(ex))

    return ProductionDataObj, Success, Messages

def GetActuals(WellorArea, Wedge, start_date, end_date, LEName = '', Adjusted = False, Phase = 'Gas'):
    from Model import ModelLayer as m
    import pandas as pd
    from Controller import SummaryModule as s
    from Model import ImportUtility as i
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx

    Success = True
    Messages = []
    ActualProduction = []
    try:
        WellList = []
        if WellorArea:
            WellList = s.GetFullWellList(WellorArea)
        if Wedge:
            wedge_list, Success, Message = GetWellorAreaByWedge(Wedge)
            if not Success:
                Messages.append('Error finding wells associated with input Wedge. ')
            else:
                WellList.extend(wedge_list)


        EDWobj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')
        corpid_query = qf.EDWKeyQueryFromWellName(WellList)
        corpid_list = EDWobj.Query(corpid_query)
        corpid_list = list(corpid_list[1]['CorpID'])
        if LEName and Adjusted == True:
            actuals_df, Success, Message = i.ImportActuals(corpid_list, pd.to_datetime(start_date), pd.to_datetime(end_date), LEName)   
        else:
            actuals_df, Success, Message = i.ImportActuals(corpid_list, pd.to_datetime(start_date), pd.to_datetime(end_date), '')
        
        if not Success:
            Messages.append(Message)
        else:
            dates = actuals_df['Date_Key'].unique()
            prod_array = []
            date_array = []
            if not actuals_df.empty:
                if Phase == 'Gas':
                    for date in dates:
                        results = actuals_df.query('Date_Key == @date')
                        prod_array.append(results['Gas'].sum())
                        date_array.append(date)
                    ActualProduction = ProductionData(date_array, prod_array, Phase,'scf')
                elif Phase == 'Water':
                    for date in dates:
                        results = actuals_df.query('Date_Key == @date')
                        prod_array.append(results['Water'].sum())
                        date_array.append(date)
                    ActualProduction = ProductionData(date_array, prod_array, Phase,'bbl')
                elif Phase == 'Oil':
                    for date in dates:
                        results = actuals_df.query('Date_Key == @date')
                        prod_array.append(results['Oil'].sum())
                        date_array.append(date)
                    ActualProduction = ProductionData(date_array, prod_array, Phase,'bbl')

    except Exception as ex:
        Success = False
        Messages.append('Error during import of Actual production data. ' + str(ex))

    return ActualProduction, Success, Messages

def SelectLEByCriteria(Wedge, WellorArea, NameFilter, start_date, end_date):
    from Model import ModelLayer as m
    import pandas as pd
    from datetime import datetime, timedelta

    Success = True
    Messages = []

    try:
        ret_LE_list = []
        if WellorArea:
            LEHeaderObj = m.LEHeader('', [WellorArea], [], [])
        else:
            LEHeaderObj = m.LEHeader('', [], [], [])

        rows, Success, Message = LEHeaderObj.ReadTable()

        if not Success:
            Messages.append(Message)
        else:
            
            if len(rows) > 0:
                df = pd.DataFrame([vars(s) for s in rows])
                pd_start_date = pd.to_datetime(start_date)
                if not pd.isnull(pd_start_date):
                    pd_end_date = pd.to_datetime(end_date)
                    if start_date and pd.isnull(pd_end_date):                        
                        end_date = pd_start_date + timedelta(days = 1)
                        end_date = end_date.strftime('%m/%d/%Y')

                    df = df.query('LE_Date >= @start_date and LE_Date <= @end_date')

                if Wedge:
                    df = df.query('Wedge == @Wedge')
                if NameFilter:
                    df = df.query('LEName.str.contains(@NameFilter)')
                
                if not df.empty:
                    ret_LE_dict = {}
                    ret_LE_dict['LEName'] = df['LEName'].values[0]
                    ret_LE_dict['LE_Date'] = df['LE_Date'].values[0]
                    ret_LE_list.append(ret_LE_dict)
                else:
                    Messages.append('No LE in database matches search criteria.')

    except Exception as ex:
        Success = False
        Messages.append('Error during selection of LE. ' + str(ex))

    return ret_LE_list, Success, Messages

def SelectForecastByCriteria(WellorArea, Wedge, NameFilter, GFOz):
    from Model import ModelLayer as m
    import pandas as pd

    Success = True
    Messages = []
    df = pd.DataFrame()
    try:
        ret_LE_list = []
        well_list = []
        if WellorArea:
            well_list.append(WellorArea)
        if Wedge:
            wedge_list, Success, Message = GetWellorAreaByWedge(Wedge)
            if not Success:
                Messages.append(Message)
                well_list = []
            else:
                well_list.extend(wedge_list)

        ForecastHeaderRowObj = m.ForecastHeader('', well_list, [], [])
        ForecastData, Success, Message = ForecastHeaderRowObj.ReadTable()

        if not Success:
            Messages.append(Message)
        elif len(ForecastData) > 0:
            df = pd.DataFrame([vars(s) for s in ForecastData])

            if NameFilter:
                df = df.query('ForecastName.str.contains(@NameFilter)')
            if GFOz:
                df = df.query('GFO == 1')

        if not df.empty:
            ret_LE_dict = {}
            ret_LE_dict['ForecastName'] = df['ForecastName'].values[0]
            ret_LE_list.append(ret_LE_dict)
        else:
            Messages.append('No Forecast in database matches search criteria.')


    except Exception as ex:
        Success = False
        Messages.append('Error during selection of Forecast. ' + str(ex))


    return ret_LE_list, Success, Messages

def GetWellorAreaByWedge(Wedge):    
    from Model import ModelLayer as m
    import pandas as pd
    from datetime import datetime

    Success = True
    Messages = []
    well_list = []

    try:
        #Query the LE tables to gather all wells listed by the Wedge name
        LEHeaderObj = m.LEHeader('', [], [], [])
        LEHeaderResults, Success, Message = LEHeaderObj.ReadTable()

        if not Success:
            Messages.append(Message)
        else:
            if len(LEHeaderResults) > 0:
                LE_df = pd.DataFrame([vars(s) for s in LEHeaderResults])

                LE_df = LE_df.query('Wedge == @Wedge')
                well_list = list(LE_df['WellName'].unique())

    except Exception as ex:
        Success = False
        Messages.append('Error during query for wells by given Wedge. ')

    return well_list, Success, Messages

class ProductionData:
    def __init__(self, dates, production_values, phase, units):
        self.dates = dates #datetime array
        self.production_values = production_values #float array
        self.phase = phase #string
        self.units = units #string

if __name__ == '__main__':

    #Tests
    # LE_list, Success, Messages = GetFirstOfMonthLE('07/01/2019', 'Kanga 2 H', '', 'Test')
    # LE_list, Success, Messages = GetLastWeekLE('07/31/2019', 'Kanga 2 H', '', 'Test')
    # name = LE_list[0]['LEName']
    # Production, Success, Messages = GetLEProduction(name, '', '', 'Gas')


    # ForecastList, Success, Messages = GetForecastName('', '2019 NWD', 'Aries', False)
    # forecastname = 'Aries_GFOz'
    # Production, Success, Messages = GetForecastProduction(forecastname, '2019 NWD', '', 'Gas')


    Production, Success, Messages = GetActuals('', '2019 NWD', '07/01/2019', '07/31/2019', LEName = '', Adjusted = False, Phase = 'Gas')
    print(Success)
    print(Messages)


#Things to plot:
# LatestLE
# FirstOfMonthLE
# LastWeek'sLE
# ForecastUsed
# GFOz
# ActualGas
# ActualOil
# ActualWater
# AdjustedGas
# AdjustedOil
# AdjustedWater
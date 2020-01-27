import sys
sys.path.append('../')

def CreateLEFromForecast(LEName, well_list, LE_Date, ForecastName, start_date, end_date, Update_User, SuppressMessages):
    from Model import BPXDatabase as bpx
    from Model import ModelLayer as m
    from Model import QueryFile as qf
    from Controller import DataInterface as di
    import pandas as pd
    from datetime import datetime, date, timedelta
    from Controller import SummaryModule as s
    from Model import ImportUtility as iu
    #Create a LE from a particular forecast
    #Create a list of date keys from start_date to end_date

    Success = True
    Messages = []

    try:
        date_list = []
        pd_start_date = pd.to_datetime(start_date)
        pd_end_date = pd.to_datetime(end_date)

        date_list.append(pd_start_date)
        delta = pd_end_date - pd_start_date
        for i in range(delta.days + 1):
            day = pd_start_date + timedelta(days=i)
            date_list.append(day)

        if not SuppressMessages:
            print('Retrieving Forecast values for given well list...')
        ForecastRowObj = m.ForecastData('', [ForecastName], well_list, date_list )
        Rows, Success, Message = ForecastRowObj.ReadTable()

        if Message:
            Messages.append(Message)

        if Success:
            #Ensure that every item in the well_list is represented and append a message if not
            ForecastRows_df = pd.DataFrame([vars(s) for s in Rows])
            for well in well_list:                
                well_results_df = ForecastRows_df.query('CorpID == @well')
                if well_results_df.empty:
                    msg = 'WARNING: ' + well + ' not represented in chosen forecast for this date range.'
                    Messages.append(msg)
                    if not SuppressMessages:
                        print(msg)
            
            #Create a new Le for every row
            if not SuppressMessages:
                print('Creating a LE for given Well List...')
            count = 0
            Header_Row = ''
            for row in Rows:
                #Create Header Object
                if Header_Row != row.CorpID:
                    #Get Well Name from corpID
                    well_query = qf.EDWKeyQueryFromCorpID([row.CorpID], '')
                    EDWObj = bpx.GetDBEnvironment('ProdEDW', 'OVERRIDE')
                    well_results = EDWObj.Query(well_query)

                    Areas = []
                    AreaObj = m.AreaAggregation('', [], [], [])
                    AreaRows, Success, Messages = AreaObj.ReadTable()
                    if len(AreaRows) > 0:
                        AreaRows = pd.DataFrame([vars(s) for s in AreaRows])
                        Areas = AreaRows['AggregateName'].unique()
                        Areas = list(Areas)

                    if not well_results[1].empty:
                        WellName = well_results[1]['WellName'].values[0]
                    elif row.CorpID in Areas:
                        WellName = row.CorpID
                    else:
                        msg = 'Skipped input due to lack of return well name. ' + row.CorpID
                        Messages.append(msg)
                        if not SuppressMessages:
                            print(msg)
                        continue

                    Wedge, Messages = iu.GetWedgeData(row.CorpID, SuppressMessages)

                    LEHeaderObj = m.LEHeaderRow(LEName, WellName, row.CorpID, ForecastName, Wedge, LE_Date, '')
                    Success, Message = LEHeaderObj.Write(Update_User, datetime.now())
                    if not Success:
                        Messages.append(Message)
                        count = count + 1
                        continue
                    Header_Row = row.CorpID

                LEDataRowObj = m.LEDataRow(LEName, row.CorpID, row.Date_Key, row.Gas_Production, row.Oil_Production, row.Water_Production, '')
                Write_Success, Message = LEDataRowObj.Write(Update_User, datetime.now())
                if not Write_Success:
                    Messages.append(Message)
                    if not SuppressMessages:
                        print(Message)

                count = count + 1
                if not SuppressMessages:
                    di.callprogressbar(count, len(Rows))
            
            #Create default Frac Hit Multipliers for the wells
            if not SuppressMessages:
                print('Generating default Frac Hit Multipliers')
            Success, Message = di.WriteDefaultMultipliers(LEName, 1, Update_User, datetime.now(), SuppressMessages)
                
    except Exception as ex:
        Success = False
        Messages.append('Error during LE Creation from Forecast. ' + str(ex))

    return Success, Messages

def CreateLEFromLE(NewLEName, WellList, GeneratedFromLEName, LE_Date, start_date, end_date, Update_User):
    from Model import BPXDatabase as bpx
    from Model import ModelLayer as m
    from Model import QueryFile as qf
    from Controller import DataInterface as di
    import pandas as pd
    from datetime import datetime, date, timedelta
    from Controller import SummaryModule as s

    Success = True
    Messages = []
    try:
        #Cycle through each row in the given LE header and create LE from that forecast        
        LEHeaderObj = m.LEHeader('', [], WellList, [GeneratedFromLEName], [])
        LEHeaderRows, Success, Message = LEHeaderObj.ReadTable()

        if Message:
            Messages.append(Message)

        print('Creating LE from existing LE...')
        if Success:
            count = 0
            for HeaderRow in LEHeaderRows:
                well_list = [HeaderRow.CorpID]
                ForecastName = HeaderRow.ForecastGeneratedFrom
                Success, Message = CreateLEFromForecast(NewLEName, well_list, LE_Date, ForecastName, start_date, end_date, Update_User, True)
                
                count = count + 1
                di.callprogressbar(count, len(LEHeaderRows))                

                if Message:
                    Messages.append(Message)
        
    except Exception as ex:
        Messages.append('Error during creation of LE. ' + str(ex))
        Success = False

    return Success, Messages

def DeleteLE(LEName):
    #When deleting an LE several steps need to be adhered to for the proper removal of the data from the database
    # 1.) Delete the Frac Hit Multipliers associated with the LE 
    # 2.) Delete the LE Rows associated with the header
    # 3.) Delete the LE Header rows associated with the passed LE Name
    from Model import ModelLayer as m

    Messages = []
    Success = True
    
    row_success = True
    header_success = True
    multiplier_success = True

    Multiplier_Rows_Obj = m.FracHitMultipliers('',  [LEName], [], [])
    Multiplier_Rows, success, msg = Multiplier_Rows_Obj.ReadTable()
    for multiplier_row in Multiplier_Rows:
        multiplier_success, message = multiplier_row.Delete()
        if not multiplier_success:
            Messages.append(message)

    LE_Rows_Obj = m.LEData('', [LEName], [], [])
    LE_Rows, success, msg = LE_Rows_Obj.ReadTable()
    for le_row in LE_Rows:
        row_success, message = le_row.Delete()
        if not row_success:
            Messages.append(message)

    Header_Rows_Obj = m.LEHeader('', [], [], [LEName], [])
    Header_Rows, success, msg = Header_Rows_Obj.ReadTable()
    for header_row in Header_Rows:
        header_success, message = header_row.Delete()
        if not header_success:
            Messages.apppend(message)

    db_list = []
    if multiplier_success and row_success and header_success:
        if Header_Rows:
            db_list.append(Header_Rows[0].DBObj)
        if LE_Rows and LE_Rows[0].DBObj not in db_list:
            db_list.append(LE_Rows[0].DBObj)
        if Multiplier_Rows and Multiplier_Rows[0].DBObj not in db_list:
            db_list.append(Multiplier_Rows[0].DBObj)
        
        for db in db_list:
            db.Command('Commit')

    return Success, Messages

def DeleteForecast(ForecastName):
    #When deleting an LE several steps need to be adhered to for the proper removal of the data from the database    
    # 1.) Delete the Data Rows associated with the header
    # 2.) Delete the Forecast Header rows associated with the passed LE Name
    from Model import ModelLayer as m
    Messages = []
    Success = True

    row_success = True
    header_success = True

    Forecast_Rows_Obj = m.ForecastData('', [ForecastName], [], [])
    Forecast_Rows, success, msg = Forecast_Rows_Obj.ReadTable()
    for forecast_row in Forecast_Rows:
        row_success, message = forecast_row.Delete()
        if not Success:
            Messages.append(message)

    Header_Rows_Obj = m.ForecastHeader('', [], [], [ForecastName])
    Header_Rows, success, msg = Header_Rows_Obj.ReadTable()
    for header_row in Header_Rows:
        header_success, message = header_row.Delete()
        if not Success:
            Messages.apppend(message)

    #Commit the delete        
    db_list = []
    if row_success and header_success:
        if Header_Rows:
            db_list.append(Header_Rows[0].DBObj)
        if Forecast_Rows and Forecast_Rows[0].DBObj not in db_list:
            db_list.append(Forecast_Rows[0].DBObj)

        for db in db_list:
            db.Command('Commit')
            
    return Success, Messages

# if __name__ == '__main__':

    # well_list = ['14364160-000'
    # ,'14376244-000'
    # ,'14376245-000'
    # ,'14384753-000'
    # ,'14385259-000'
    # ,'14385419-000'
    # ,'14396384-000'
    # ,'14402359-000'
    # ,'14402559-000'
    # ,'14404208-000'
    # ,'14404359-000'
    # ,'14406376-000'
    # ,'13071342-000'
    # ,'13387002-000'
    # ,'13763119-000'
    # ,'13860389-001'
    # ,'14248578-000'
    # ,'14248579-000'
    # ,'14258600-000'
    # ,'14269983-000'
    # ,'14295238-000'
    # ,'14295819-000'
    # ,'14303813-000'
    # ,'14309276-000'
    # ,'14309562-000'
    # ,'14312669-000'
    # ,'14313648-000'
    # ,'14313869-000'
    # ,'14313988-000'
    # ,'14316545-000'
    # ,'14319343-000'
    # ,'14319899-001'
    # ,'14319900-001'
    # ,'14322612-001'
    # ,'14322613-001'
    # ,'14322672-000'
    # ,'14325314-000'
    # ,'14326646-001'
    # ,'14326647-001'
    # ,'14340136-001'
    # ,'14340216-001'
    # ,'14340217-001'
    # ,'14354532-002'
    # ,'14364160-000'
    # ,'14365559-000'
    # ,'14365684-000'
    # ,'14365685-000'
    # ,'14367479-000'
    # ,'14367849-000'
    # ,'14368525-000'
    # ,'14374279-001'
    # ,'14374999-000'
    # ,'14376193-000'
    # ,'14376194-000'
    # ,'14376244-000'
    # ,'14376245-000'
    # ,'14376246-000'
    # ,'14379956-000'
    # ,'14381121-000'
    # ,'14381815-000'
    # ,'14384752-000'
    # ,'14384753-000'
    # ,'14385259-000'
    # ,'14385419-000'
    # ,'14396384-000'
    # ,'14402359-000'
    # ,'14402559-000'
    # ,'14404208-000'
    # ,'14404359-000'
    # ,'14406376-000'
    # ,'Angie'
    # ,'Apato 1 H'
    # ,'Apato 2 H'
    # ,'Blocker'
    # ,'Bronto 3 H'
    # ,'Bronto 3 H B'
    # ,'Denali 3 H'
    # ,'East Carthage'
    # ,'Everest 1 H'
    # ,'Fox'
    # ,'Glenwood'
    # ,'Nash'
    # ,'Oak Hill'
    # ,'Rainier 2 H'
    # ,'Rainier 2 H B'
    # ,'Stockman'
    # ,'Trex 4 H U'
    # ,'Ttops 2 H B'
    # ,'Vulcanodon 1 H'
    # ,'Vulcanodon 1 H B'
    # ,'Walker J 2 H'
    # ,'Walker J 2 H B'
    # ,'West Carthage'
    # ,'Woodlawn']

    # well_list = ['Angie' ]

    # Sucess, Message = CreateLEFromForecast('Test_Create_Total_July', well_list, '07/22/2019' , 'Travis_Internal_GFOForecast', '07/01/2019', '07/31/2019', 'Travis Comer', False)
    # Success, Message = CreateLEFromLE('Test_Create_From_LE', [], 'Test_Create_Total_July', '09/30/2019', '09/01/2019', '09/30/2019', 'Travis Comer' )
 
    # print(Success)


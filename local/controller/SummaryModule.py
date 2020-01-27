"""
***************************************************************************************
   Description: This module is designed to perform 'LE' summary calculations and store
   them for future use
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


def CalculateSummaryInfo(LEName, ForecastName, SummaryName, SummaryDate, Update_User):
    from Model import ModelLayer as m
    from Model import BPXDatabase as bpx
    from Model import ImportUtility as i
    import numpy as np
    import pandas as pd
    from datetime import datetime
    from datetime import timedelta
    import calendar
    from Controller import DataInterface as di
    

    Success = True
    Messages = []

    try:
        #Get netting factor for each well
        #Calculate the Monthly AveragMBOED

        print('Gathering wedge information...')
        LEHeaderObj = m.LEHeader('', [], [], [LEName], [])
        Rows, Success, Message = LEHeaderObj.ReadTable()

        LEDataRowObj = m.LEData('', [LEName], [], [])
        DataRows, Success, Message = LEDataRowObj.ReadTable()
        if not Success or len(DataRows) == 0:
            if not Message:
                Message = 'No LE found in the database.'
            Messages.append(Message)
        else:
            well_list = []
            for row in Rows:
                well_list.append(row.WellName)

            NettingObj = m.GasNetting('', well_list, [])
            NettingRows, Success, Message = NettingObj.ReadTable()

            if isinstance(SummaryDate, str):
                today = pd.to_datetime(SummaryDate)
            elif isinstance(SummaryDate, datetime):
                today = SummaryDate
            else:
                today = datetime.today()

            first_of_month = datetime(today.year, today.month, 1)
            first_of_year = datetime(today.year, 1, 1)

            quarter_start = pd.to_datetime(today - pd.tseries.offsets.QuarterBegin(startingMonth = 1)).date()
            quarter_end = pd.to_datetime(today + pd.tseries.offsets.QuarterEnd(startingMonth = 3)).date()
            next_quarter_start = quarter_end + timedelta(days = 1)
            month_end = pd.to_datetime(today + pd.tseries.offsets.MonthEnd(1)).date()

            tomorrow = today + timedelta(days=1)
            year_end = datetime(today.year, 12, 31)

            Rows = pd.DataFrame([vars(s) for s in Rows])
            DataRows = pd.DataFrame([vars(s) for s in DataRows])

            #This will be the end of the LE forecast plus one day.
            next_day_date = max(DataRows['Date_Key']) + timedelta(days=1)
            le_start_date = min(DataRows['Date_Key'])
            le_end_date = max(DataRows['Date_Key'])
            
            #Create a dataframe that calculates total and also line item by wedge
            #loop through each wedge
            wedge_list = Rows['Wedge'].unique()
            wedge_totals = []
            for wedge in wedge_list:                          #Eventually, may need to add oil and water in this as other phases are implemented
                wedge_total = {}
                wedge_rows = Rows.query('Wedge == @wedge')
                #Gather well list for this particular wedge and obtain:
                # 1.) Actuals up to current day
                # 2.) LE Forecast values to end of month
                #     - if LE Forecast does not extend to end of month, send warning message and get GFO values to end of year
                # 3.) GFO Forecast values from first of next month to year end.
                # 4.) Average all of the values and multiply by Netting Value and Frac Hit Multipliers for that forecast.
                #     - If no frac hit multipliers exist, default to 1. (send message)
                #     - If no Netting Values exist, default to 1. (send message)

                print('Importing wedge actuals...')
                corpIDs = wedge_rows['CorpID'].unique()
                corpIDs = list(corpIDs)
                all_corpids = i.GetFullWellList(corpIDs)
                ytd_actuals_df, Success, Messages = i.ImportActuals(all_corpids, first_of_year, today, LEName)    

                first_of_month_date_only = datetime.date(first_of_month)
                today_date_only = datetime.date(today)                 

                mtd_actuals_df = ytd_actuals_df.query('Date_Key >= @first_of_month_date_only and Date_Key < @today_date_only')
                qtd_actuals_df = ytd_actuals_df.query('Date_Key >= @quarter_start and Date_Key < @today_date_only')

                now = datetime.now()
                days_in_month = calendar.monthrange(now.year, now.month)[1]
                days_in_quarter = (quarter_end - quarter_start).days
                days_in_year = 365

                print('Gathering wedge GFO data for \'' + wedge + '\' ...')

                gfo_annually_wedge_df, Success, Message = GetGFOValues(ForecastName, corpIDs, first_of_year, year_end)
                if not Success:
                    Messages.append(Message)
                else:                       
                    gfo_monthly_wedge_df = gfo_annually_wedge_df.query('Date_Key >= @first_of_month and Date_Key <= @month_end')                    
                    gfo_quarterly_wedge_df = gfo_annually_wedge_df.query('Date_Key >= @quarter_start and Date_Key <= @quarter_end')

                if Success:
                    #Generate Daily Netting Values based on table entries for each CorpID
                    GasNettingObj = m.GasNetting('', [], all_corpids)
                    GasNettingValues, Success, Message = GasNettingObj.ReadTable()
                    if len(GasNettingValues) > 0:
                        GasNettingValues = pd.DataFrame([vars(s) for s in GasNettingValues])
                        Gas_Daily_Nf = GenerateDailyNF(GasNettingValues, first_of_year, year_end)
                        Gas_Daily_Nf.rename(columns = {'NettingValue':'GasNF'}, inplace = True)
                    else:
                        Gas_Daily_Nf = pd.DataFrame(columns = ['GasNF', 'Date_Key', 'CorpID'])

                    OilNettingObj = m.OilNetting('', [], all_corpids)
                    OilNettingValues, Success, Message = OilNettingObj.ReadTable()
                    if len(OilNettingValues) > 0:
                        OilNettingValues = pd.DataFrame([vars(s) for s in OilNettingValues])
                        Oil_Daily_Nf = GenerateDailyNF(OilNettingValues, first_of_year, year_end)
                        Oil_Daily_Nf.rename(columns = {'NettingValue':'OilNF'}, inplace = True)
                    else:
                        Oil_Daily_Nf = pd.DataFrame(columns = ['OilNF', 'Date_Key', 'CorpID'])

                    #Merge the NF tables
                    daily_Nf = pd.merge(Gas_Daily_Nf, Oil_Daily_Nf, left_on =['Date_Key', 'CorpID'], right_on =['Date_Key', 'CorpID'], how = 'outer')
                    daily_Nf.fillna(0, inplace = True)

                    summary = []
                    if not Success:
                        Messages.append(Message)
                    else:
                        MultipliersObj = m.FracHitMultipliers('', [LEName], [], [])
                        MultiplierRows, Success, Message = MultipliersObj.ReadTable()
                        if  MultiplierRows:
                            MultiplierRows = pd.DataFrame([vars(s) for s in MultiplierRows])

                        print('Calculating wedge summary information for \'' + wedge + '\' ...')
                        count = 1                         
                        for corpID in corpIDs:
                            row = {}
                            well_df = Rows.query('CorpID == @corpID')
                            row['WellName'] = well_df['WellName'].iloc[0]                    
                            row['CorpID'] = corpID
                            row['Wedge'] = well_df['Wedge'].iloc[0]
                            area_corpids = i.GetFullWellList([corpID])
                            
                            #Monthly data
                            #loop through actuals
                            well_mtd_actual_df = GetActualsFromWellorArea(mtd_actuals_df,  area_corpids)
                            well_ytd_actual_df = GetActualsFromWellorArea(ytd_actuals_df,  area_corpids)
                            well_qtd_actual_df = GetActualsFromWellorArea(qtd_actuals_df,  area_corpids)

                            #Get Netting Factors and Multipliers from Frac Hit Mitigation
                            well_multipliers_df = pd.DataFrame()                            
                            annual_default_multipliers_df = GenerateDefaultMultipliers(1, first_of_year, year_end)
                            if not MultiplierRows.empty:
                                well_multipliers_df = MultiplierRows.query('CorpID == @corpID')
                            else:
                                Messages.append('No multiplier values found, using default value of 1.')
                                well_multipliers_df = annual_default_multipliers_df

                                                             
                            well_mtd_actual_sum = SumDailyValues(well_mtd_actual_df, daily_Nf, annual_default_multipliers_df, 'MeasuredGas', 'MeasuredOil' )
                            well_ytd_actual_sum = SumDailyValues(well_ytd_actual_df, daily_Nf, annual_default_multipliers_df, 'MeasuredGas', 'MeasuredOil')
                            well_qtd_actual_sum = SumDailyValues(well_qtd_actual_df, daily_Nf, annual_default_multipliers_df, 'MeasuredGas', 'MeasuredOil')

                            well_gfo_monthly_df = gfo_monthly_wedge_df.query('CorpID == @corpID')
                            well_gfo_quarterly_df = gfo_quarterly_wedge_df.query('CorpID == @corpID')
                            well_gfo_annually_df = gfo_annually_wedge_df.query('CorpID == @corpID')

                            well_forecast_name = well_df['ForecastGeneratedFrom'].iloc[0]
                            gfo_after_le_to_yearend_df, Success, Message = GetGFOValues(well_forecast_name, corpIDs, next_day_date, year_end)
                            gfo_after_le_to_month_df = gfo_after_le_to_yearend_df.query('Date_Key >= @next_day_date and Date_Key <= @month_end')
                            gfo_after_le_to_quarter_df = gfo_after_le_to_yearend_df.query('Date_Key >= @next_day_date and Date_Key <= @quarter_end')
                        
                            well_gfo_after_le_month_df = gfo_after_le_to_month_df.query('CorpID == @corpID')
                            well_gfo_after_le_quarter_df = gfo_after_le_to_quarter_df.query('CorpID == @corpID')
                            well_gfo_after_le_annual_df = gfo_after_le_to_yearend_df.query('CorpID == @corpID')

                            #Get GFO Sums
                            well_gfo_monthly_sum_midmonth = GetMidMonthGFOValue(well_gfo_monthly_df, 'Gas_Production', 'Oil_Production')
                            well_gfo_quarterly_sum_midmonth = GetMidMonthGFOValue(well_gfo_quarterly_df, 'Gas_Production', 'Oil_Production')
                            well_gfo_annually_sum_midmonth = GetMidMonthGFOValue(well_gfo_annually_df, 'Gas_Production', 'Oil_Production')

                            #Convert NF to method expected dataframe from forecast table
                            forecast_NF_dict = {}
                            # forecast_NF_dict['GasNF'] = well_gfo_after_le_annual_df['GasNF'].values
                            # forecast_NF_dict['OilNF'] = well_gfo_after_le_annual_df['OilNF'].values
                            forecast_NF_dict['Date_Key'] = well_gfo_after_le_annual_df['Date_Key'].values
                            forecast_NF_dict['CorpID'] = [corpID] * well_gfo_after_le_annual_df.shape[0]
                            forecast_NF_df = pd.DataFrame(forecast_NF_dict)

                            well_gfo_after_le_month_sum = SumDailyValues(well_gfo_after_le_month_df, forecast_NF_df, annual_default_multipliers_df, 'Gas_Production', 'Oil_Production')
                            well_gfo_after_le_quarter_sum = SumDailyValues(well_gfo_after_le_quarter_df, forecast_NF_df, annual_default_multipliers_df, 'Gas_Production', 'Oil_Production')
                            well_gfo_after_le_annual_sum = SumDailyValues(well_gfo_after_le_annual_df, forecast_NF_df, annual_default_multipliers_df, 'Gas_Production', 'Oil_Production')

                            well_le_df_all = DataRows.query('CorpID == @corpID')
                            well_le_df_month = well_le_df_all.query('Date_Key >= @today_date_only and Date_Key <= @month_end')
                            well_le_df_total = well_le_df_all.query('Date_Key > @month_end')
                            well_le_sum_total = SumDailyValues(well_le_df_total, daily_Nf, well_multipliers_df, 'Gas_Production', 'Oil_Production' )
                            well_le_sum_month = SumDailyValues(well_le_df_month, daily_Nf, well_multipliers_df, 'Gas_Production', 'Oil_Production')
                            
                            row['LE_Monthly'] = (well_mtd_actual_sum + well_le_sum_month + well_gfo_after_le_month_sum) 
                            row['GFOzMonthly'] = well_gfo_monthly_sum_midmonth 
                            row['LE_Quarterly'] = (well_qtd_actual_sum + well_le_sum_month + well_le_sum_total + well_gfo_after_le_quarter_sum) 
                            row['GFOzQuarterly'] = (well_gfo_quarterly_sum_midmonth) 
                            row['LE_Annually'] = (well_ytd_actual_sum + well_le_sum_month + well_le_sum_total + well_gfo_after_le_annual_sum) 
                            row['GFOzAnnually'] = (well_gfo_annually_sum_midmonth) 
                            summary.append(row)

                            di.callprogressbar(count, len(corpIDs))
                            count = count + 1

                        print('Summarizing wedge data...')
                        if summary:                        
                            summary_df = pd.DataFrame(summary)
                            length = summary_df.shape[0]
                            wedge_total['wedge_name'] = wedge
                            wedge_total['LE_Monthly'] = summary_df['LE_Monthly'].sum() / days_in_month / 1000
                            wedge_total['GFOzMonthly'] = summary_df['GFOzMonthly'].sum() / days_in_month / 1000
                            wedge_total['MonthlyVariance'] = wedge_total['LE_Monthly'] - wedge_total['GFOzMonthly'] 
                            wedge_total['LE_Quarterly'] = summary_df['LE_Quarterly'].sum() / days_in_quarter / 1000
                            wedge_total['GFOzQuarterly'] = summary_df['GFOzQuarterly'].sum() / days_in_quarter / 1000
                            wedge_total['QuarterlyVariance'] = wedge_total['LE_Quarterly']  - wedge_total['GFOzQuarterly'] 
                            wedge_total['LE_Annually'] = summary_df['LE_Annually'].sum() / days_in_year / 1000
                            wedge_total['GFOzAnnually'] = summary_df['GFOzAnnually'].sum() / days_in_year / 1000
                            wedge_total['AnnualVariance'] = wedge_total['LE_Annually'] - wedge_total['GFOzAnnually'] 
                            wedge_totals.append(wedge_total)                        
            
            print('Writing to database...')
            for row in wedge_totals:
                #Create a Summary entry
                WedgeName = row['wedge_name']
                Midstream = ''
                Reason = ''
                Comments = ''
                LEName = LEName
                GFOForecastName = ForecastName
                MonthlyAvgMBOED = row['LE_Monthly']
                QuarterlyAvgMBOED = row['LE_Quarterly']
                AnnualAvgMBOED = row['LE_Annually']
                MonthlyGFOMBOED = row['GFOzMonthly']
                QuarterlyGFOMBOED = row['GFOzQuarterly']
                AnnualGFOMBOED = row['GFOzAnnually']
                MonthlyVariance = row['MonthlyVariance']
                QuarterlyVariance = row['QuarterlyVariance']
                AnnualVariance = row['AnnualVariance']
                SummaryObj = m.LESummaryRow(SummaryName, WedgeName, Midstream, Reason, Comments, SummaryDate, LEName, GFOForecastName, MonthlyAvgMBOED, QuarterlyAvgMBOED, 
                AnnualAvgMBOED, MonthlyGFOMBOED, QuarterlyGFOMBOED, AnnualGFOMBOED, MonthlyVariance, QuarterlyVariance, AnnualVariance, '')

                Write_Success, Message = SummaryObj.Write(Update_User, datetime.now())
                if not Write_Success:
                    Messages.append(Message)

    except Exception as ex:
        Sucess = False
        Messages.append('Error during calculation of summary information. ' + str(ex))

    return Success, Messages

def GenerateDefaultMultipliers(default_val, start_date, end_date):
    import pandas as pd
    from datetime import datetime, timedelta

    #Generate a daily data frame with default_val as the multiplier
        #Logic for creating the nettingValues and Multiplier dataframes
    delta = end_date - start_date
    dates = []
    values = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days = i)
        values.append(default_val)
        dates.append(day)

    multipliers_df = pd.DataFrame()
    multipliers_df['Date_Key'] = dates
    multipliers_df['Multiplier'] = values

    return multipliers_df

def GenerateDailyNF(nf_df, start_date, end_date):
    import pandas as pd
    from datetime import datetime, timedelta

    #Logic for creating the daily nettingValues dataframes
    dates = []
    values = []
    corpID_list = []
    corpIDs = list(nf_df['CorpID'].unique())
    iter_dates = pd.date_range(start_date, end_date).to_list()
    for item in corpIDs:
        value_df = nf_df.query('CorpID == @item')
        
        if value_df.shape[0] > 1:
            for day in iter_dates:
                value_df = value_df.query('NettingDate <= @day')
                value_df.sort_values(by = 'NettingDate', ascending = False)
                value = value_df['NettingValue'].iloc[0]
                values.append(value)
                corpID_list.append(item)
                dates.append(day)
        elif value_df.shape[0] == 1:
            values.extend([value_df['NettingValue'].values[0]] * len(iter_dates))
            corpID_list.extend([item] * len(iter_dates))
            dates.extend(iter_dates)
        else:
            values.extend([0] * len(iter_dates))
            corpID_list.extend([item] * len(iter_dates))
            dates.extend(iter_dates)

    daily_nf_df = pd.DataFrame()
    daily_nf_df['Date_Key'] = dates
    daily_nf_df['NettingValue'] = values
    daily_nf_df['CorpID'] = corpID_list

    return daily_nf_df

def SumDailyValues(value_df, nettingValues, multipliers, gas_sum_field, oil_sum_field):
    import pandas as pd
    from datetime import datetime, timedelta
    #To improve performance, the approach will be to create a multiplier and netting factor data frame to join on date key, then use pandas
    #library for vector summary and calculations

    value_sum = 0
    #Join nettingValues and multipliers on date key
    value_df['Date_Key'] = pd.to_datetime(value_df['Date_Key'])
    nettingValues['Date_Key'] = pd.to_datetime(nettingValues['Date_Key'])
    multipliers['Date_Key'] = pd.to_datetime(multipliers['Date_Key'])

    #Merge netting on the Date Key and the Netting Factor (that is on the individual well level, even if area aggregated in LE)
    value_df = pd.merge(value_df, nettingValues, left_on =['Date_Key', 'CorpID'], right_on =['Date_Key', 'CorpID'])

    #Merge multipliers on Date Key only, since these would be managed separately according to the Area Aggregation in the LE adjustment
    value_df = pd.merge(value_df, multipliers, on = 'Date_Key', how = 'left')

    if not value_df.empty:
        value_df['Calculated'] = (value_df[gas_sum_field] * value_df['GasNF'] + value_df[oil_sum_field] * value_df['OilNF'])* value_df['Multiplier']

        value_sum = value_df['Calculated'].sum()

    return value_sum

def GetGFOValues(ForecastName, WellList, StartDate, EndDate):
    from Model import ModelLayer as m
    import pandas as pd
    from datetime import datetime
    from datetime import date

    Success = True
    Messages = []
    GFO_df = []
    try:
        #NEW Column in Header: Netting Factor. Query For this value and append to the returned GFO_df
        ForecastObj = m.ForecastData('', [ForecastName], WellList, [])
        ForecastHdrObj = m.ForecastHeader('', [], WellList, [ForecastName])
        Rows, Success, Message = ForecastObj.ReadTable()
        HdrRows, Success, Message = ForecastHdrObj.ReadTable()
        if not Success:
            Messages.append(Message)
        else:
            GFO_df = pd.DataFrame([vars(s) for s in Rows])
            GFO_hdr_df = pd.DataFrame([vars(s) for s in HdrRows])

            if isinstance(StartDate, date):
                StartDate = datetime.combine(StartDate, datetime.min.time())

            if isinstance(EndDate, date):
                EndDate = datetime.combine(EndDate, datetime.min.time())
                
            GFO_df = GFO_df.query('Date_Key >= @StartDate and Date_Key <= @EndDate')
            pd.merge(GFO_df, GFO_hdr_df, on='CorpID')
    
    except Exception as ex:
        Success = False
        Messages.append('Error attempting to obtain Forecast Values. ' + str(ex))
    
    return GFO_df, Success, Messages

def GetMidMonthGFOValue(forecast_df, gas_sum_field, oil_sum_field ):
    from datetime import datetime
    from calendar import monthrange
    import math
    import pandas as pd
    #This method is meant to grab the middle of the month value of recorded GFO. This 'mid' value will represent the average month daily rate.
    
    #look at the forecast data and separate out the months
    dates = forecast_df['Date_Key']
    str_dates = pd.to_datetime(dates.values).strftime('%Y/%m')
    str_dates = str_dates.unique()

    value_sum = 0
    for date in str_dates:
        fmt_date = datetime.strptime(date, '%Y/%m')

        weekday, days_in_month = monthrange(fmt_date.year, fmt_date.month)
        first_of_month = datetime(year = fmt_date.year, month = fmt_date.month, day = 1)
        last_of_month = datetime(year = fmt_date.year, month = fmt_date.month, day = days_in_month)
        #Get all values of dataframe of this month/year
        subset_df = forecast_df.query('Date_Key >= @first_of_month and Date_Key <= @last_of_month')
        mid_day = datetime(year = fmt_date.year, month = fmt_date.month, day= math.floor(days_in_month / 2))
        value_row = forecast_df.query('Date_Key == @mid_day')
        gasnettingValue = forecast_df['GasNF'].values[0]
        oilnettingValue = forecast_df['OilNF'].values[0]


        value_sum = value_sum + days_in_month * float(value_row[gas_sum_field]) * gasnettingValue + days_in_month * float(value_row[oil_sum_field]) * oilnettingValue 
             
    return value_sum

def GetActualsFromWellorArea(input_df, valid_wells):
    import pandas as pd
    #Get the actual values from either a well ID (CorpID) or an aggregated area
    #Determine if the input is a corpID or an area name

    df = pd.DataFrame()

    for item in valid_wells:
        item_df = input_df.query('CorpID == @item')
        df = df.append(item_df)

    return df

if __name__ == '__main__':
    #Test Calculate summary
    from datetime import datetime

    Success, Messages = CalculateSummaryInfo('Test_Create_Total_July', 'Travis_Internal_GFOForecast', 'Total_Test_Summary_July_5', '07/22/2019', 'Travis Comer')

    print(Messages)
    print(Success)





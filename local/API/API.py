import sys
sys.path.append('../')

from Controller import DataInterface as di
from Controller import SplashGraphModule as sgm
from Controller import SummaryModule as sm
from Controller import FracHitModule as fhm
from Controller import LatestEstimate as le
from flask import Flask, request, jsonify

from Model import ModelLayer as m
from Model import ImportUtility as iu
from Model import BPXDatabase as bpx
from Model import QueryFile as qf

from datetime import datetime
import pandas as pd
import json as js


app = Flask(__name__)

@app.route('/AriesConversion', methods=['POST'])
def CallAriesConversion():
    """Interface Package Description"""
    interface = {
    "AriesScenarioName": str,
    "ForecastName": str,
    "StartDate": datetime,
    "EndDate": datetime,
    "ForecastYear": int,    
    "Area": str,
    "GFO": bool,
    "CorpIDList": list,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        scenarioName = pkg['AriesScenarioName']
        ForecastName = pkg['ForecastName']
        ForecastYear = pkg['ForecastYear']
        start_date = pkg['StartDate']
        end_date = pkg['EndDate']
        User = pkg['UserName']
        Area = pkg['Area']
        GFO = pkg['GFO']
        CorpID = pkg['CorpIDList']

        success, msg = di.WriteAriesScenarioToDB(scenarioName, ForecastName, ForecastYear, start_date, end_date, User, Area, GFO, CorpID)
        if not success:
            del_success, del_msg = le.DeleteForecast(ForecastName)

        output = ConfigureOutput('', success, msg)
    return output

@app.route('/AreaFromRoute', methods=['POST'])
def CallCreateAreaFromRoute():
    """Interface Package Description"""
    interface = {
    "NewRouteName": str,
    "DBRouteName": str,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        new_route_name = pkg['NewRouteName']
        db_route_name = pkg['DBRouteName']
        Update_User = pkg['UserName']

        success, msg = di.CreateAreaWellsFromRoute(new_route_name, db_route_name, Update_User)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/GetLE', methods=['GET'])
def CallGetLE():
    """Interface Package Description"""
    interface = {
    "StartDate": datetime,
    "EndDate" : datetime,
    "LastWeek": bool,
    "FirstOfMonth": bool,
    "WellorArea": str,
    "Wedge": str,
    "NameFilter": str
    }

    LE = []
    pkg, success, msg = InitializePayload(request, interface)

    if success:
        StartDate = pkg['StartDate']
        EndDate = pkg['EndDate']
        WellorArea = pkg['WellorArea']
        Wedge = pkg['Wedge']
        NameFilter = pkg['NameFilter']
        LastWeek = pkg['LastWeek']
        FirstOfMonth = pkg['FirstOfMonth']

        if LastWeek:
            LE, success, msg = sgm.GetLastWeekLE(StartDate, WellorArea, Wedge, NameFilter)
        elif FirstOfMonth:
            LE, success, msg = sgm.GetFirstOfMonthLE(StartDate, WellorArea, Wedge, NameFilter)
        else:
            LE, success, msg = sgm.SelectLEByCriteria(Wedge, WellorArea, NameFilter, StartDate, EndDate)

    output = ConfigureOutput(LE, success, msg)
    return output

@app.route('/GetLEValues', methods=['GET'])
def CallLEValues():
    """Interface Package Description"""
    interface = {
    "Wedge": str,
    "WellorArea": str,
    "Name": str,
    "Phase": ['Gas', 'Oil', 'Water']
    }

    prodData = []
    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['Name']
        Wedge = pkg['Wedge']
        WellorArea = pkg['WellorArea']
        Phase = pkg['Phase']

        prodData, success, msg = sgm.GetLEProduction(LEName, Wedge, [WellorArea], Phase)

    output = ConfigureOutput(prodData, success, msg)
    return output

@app.route('/GetForecast', methods=['GET'])
def CallGetForecast():
    """Interface Package Description"""
    interface = {
    "WellorArea": str,
    "Wedge": str,
    "NameFilter": str,
    "GFOz": bool
    }

    Forecast = []
    pkg, success, msg = InitializePayload(request, interface)

    if success:
        WellorArea = pkg['WellorArea']
        Wedge = pkg['Wedge']
        NameFilter = pkg['NameFilter']
        GFOz = pkg['GFOz']

        Forecast, success, msg = sgm.SelectForecastByCriteria(WellorArea, Wedge, NameFilter, GFOz)

    output = ConfigureOutput(Forecast, success, msg)
    return output

@app.route('/GetForecastValues', methods=['GET'])
def CallGetForecastValues():
    """Interface Package Description"""
    interface = {
    "ForecastName": str,
    "Wedge": str,
    "WellorArea": str,
    "Phase": ['Gas', 'Oil', 'Water']
    }

    prodDate = []
    pkg, success, msg = InitializePayload(request, interface)

    if success:
        ForecastName = pkg['ForecastName']
        Wedge = pkg['Wedge']
        WellorArea = pkg['WellorArea']
        Phase = pkg['Phase']

        prodData, success, msg = sgm.GetForecastProduction(ForecastName, Wedge, WellorArea, Phase)

    output = ConfigureOutput(prodData, success, msg)
    return output

@app.route('/ActualProduction', methods=['GET'])
def CallActualProductionValues():
    """Interface Package Description"""
    interface = {
    "WellorArea": str,
    "Wedge": str,
    "StartDate": datetime,
    "EndDate": datetime,
    "LEName": str,
    "AdjustedBool": bool,
    "Phase": ['Gas', 'Oil', 'Water']
    }

    pkg = js.loads(request.data)
    prodData = []
    pkg, success, msg = ValidatePackage(pkg, interface)

    if success:
        WellorArea = pkg['WellorArea']
        Wedge = pkg['Wedge']
        start_date = pkg['StartDate']
        end_date = pkg['EndDate']
        LEName = pkg['LEName']
        Adjusted = pkg['AdjustedBool']
        Phase = pkg['Phase']

        prodData, success, msg = sgm.GetActuals([WellorArea], [Wedge], start_date, end_date, LEName, Adjusted, Phase)

    output = ConfigureOutput(prodData, success, msg)
    return output

@app.route('/CalculateSummary', methods=['POST'])
def CallSummaryCalculation():
    """Interface Package Description"""
    interface = {
    "LEName": str,
    "ForecastName": str,
    "SummaryName": str,
    "SummaryDate": datetime,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['LEName']
        ForecastName = pkg['ForecastName']
        SummaryName = pkg['SummaryName']
        SummaryDate = pkg['SummaryDate']
        Update_User = pkg['UserName']

        SummaryDate = SummaryDate.strftime('%m/%d/%Y')
        success, msg = sm.CalculateSummaryInfo(LEName, ForecastName, SummaryName, SummaryDate, Update_User)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/GetSummary', methods=['GET'])
def CallGetSummary():
    """Interface Package Description"""
    interface = {
    "SummaryName": list,
    "LEName": list,
    "Wedge": list,
    "GFOForecastName": list
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        SummaryName = pkg['SummaryName']
        LEName = pkg['LEName']
        Wedge = pkg['Wedge']
        GFOForecastName = pkg['GFOForecastName']

        table_obj = m.LESummary('', SummaryName, LEName, Wedge, GFOForecastName)
        results, success, msg = table_obj.ReadTable()

    output = ConfigureOutput(results, success, msg)
    return output

@app.route('/UpdateProduction', methods=['POST'])
def CallUpdateProduction():
    """Interface Package Description"""
    interface = {
    "LEName" : str,
    "CorpID" : str,
    "WellorArea": str,
    "ProductionGas": list,
    "ProductionOil": list,
    "ProductionWater":list,
    "Dates": list,
    "UserName": str
    }
    
    pkg, success, msg = InitializePayload(request, interface)
    msgs = []
    if success:
        dates = pkg['Dates']
        oil_production = pkg['ProductionOil']
        gas_production = pkg['ProductionGas']
        water_production = pkg['ProductionWater']
        UserName = pkg['UserName']
        WellName = pkg['WellorArea']
        CorpID = pkg['CorpID']
        LEName = pkg['LEName']

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)

        idx = 0
        for date in dates:
            #Create object and update
            if oil_production:
                oil = oil_production[idx]
            else:
                oil = ''
            if gas_production:
                gas = gas_production[idx]
            else:
                gas = ''            
            if water_production:
                water = water_production[idx]
            else:
                water = ''      

            row = m.ProductionAdjustmentsRow(LEName, WellName, CorpID, date, gas, oil, water, '')
            success, msg = row.Update(UserName, datetime.now())
            if not success:
                msgs.append(msg)
            else:
                msgs.append(CorpID + ' : ' + date + ' successfully updated prodcution value.')

            idx = idx + 1

    output = ConfigureOutput('', success, msgs)
    return output

@app.route('/UpdateFracMultipliers', methods=['POST'])
def CallUpdateFracMultiplier():
    """Interface Package Description"""
    interface = {
    "WellorArea": str,
    "CorpID" : str,
    "LEName": str,
    "Multipliers": list,
    "Dates": list,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)
    msgs = []
    if success:
        LEName = pkg['LEName']
        WellName = pkg['WellorArea']
        CorpID = pkg['CorpID']
        Dates = pkg['Dates']
        Multipliers = pkg['Multipliers']
        UserName = pkg['UserName']

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)

        idx = 0
        for date in Dates:
            row = m.FracHitMultipliersRow(LEName, CorpID, date, Multipliers[idx], '')
            success, msg = row.Update(UserName, datetime.now())
            if not success:
                msgs.append(msg)
            else:
                msgs.append(CorpID + ' : ' + date + ' successfully updated frac hit multiplier.')
            idx = idx + 1

    output = ConfigureOutput('', success, msgs)
    return output

@app.route('/AutoCreateFracHitMultiplier', methods=['POST'])
def CallAutoFracMultiplier():
    """Interface Package Description"""
    interface = {
    "LEName": str,
    "EastWestRadius":float,
    "NorthSouthRadius":float,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['LEName']
        EastWestFracHitRadius = pkg['EastWestRadius']
        NorthSouthFracHitRadius = pkg['NorthSouthRadius']
        Update_User = pkg['UserName']

        success, msg = fhm.FracHatMitigation(LEName, EastWestFracHitRadius, NorthSouthFracHitRadius, Update_User)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/CreateLE', methods=['POST'])
def CallCreateNewLE():
    """Interface Package Description"""
    interface = {
    "NewLEName": str,
    "Wells": list,
    "LEDate": datetime,
    "ForecastName": str,
    "OriginLEName": str,
    "StartDate": datetime,
    "EndDate": datetime,
    "UserName": str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        NewLEName = pkg['NewLEName']
        WellList = pkg['Wells']
        GeneratedFromLEName = pkg['OriginLEName']
        ForecastName = pkg['ForecastName']
        LE_Date = pkg['LEDate']
        start_date = pkg['StartDate']
        end_date = pkg['EndDate']
        Update_User = pkg['UserName']
        
        if GeneratedFromLEName:
            success, msg = le.CreateLEFromLE(NewLEName, WellList, GeneratedFromLEName, LE_Date, start_date, end_date, Update_User)
        elif ForecastName:
            success, msg = le.CreateLEFromForecast(NewLEName, WellList, LE_Date, ForecastName, start_date, end_date, Update_User, True)

        if not success:
            #Rollback creation
            del_success, del_msg = le.DeleteLE(LEName)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/DeleteLE', methods=['DELETE'])
def CallDeleteLE():
    """Interface Package Description"""
    interface = {
    "LEName" : str
    }

    pkg, success, msg = InitializePayload(request, interface)
    if success:
        LEName = pkg["LEName"]
        success, msg = le.DeleteLE(LEName)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/DeleteForecast', methods=['DELETE'])
def CallDeleteForecast():
    """Interface Package Description"""
    interface = {
    "ForecastName" : str
    }

    pkg, success, msg = InitializePayload(request, interface)
    if success:
        ForecastName = pkg["ForecastName"]
        success, msg = le.DeleteForecast(ForecastName)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/UpdateLE', methods=['POST'])
def CallUpdateLERow():
    """Interface Package Description"""
    interface = {
        "HeaderName" : str,
        "CorpID" : str,
        "Dates" : list,
        "Gas_Production" : list,
        "Oil_Production" : list,
        "Water_Production" : list,
        "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)
    msgs = []
    if success:
        HeaderName = pkg['HeaderName']
        CorpID = pkg['CorpID']
        Dates = pkg['Dates']
        Gas_Production = pkg['Gas_Production']
        Oil_Production = pkg['Oil_Production']
        Water_Production = pkg['Water_Production']
        UpdateUser = pkg['UpdateUser']

        idx = 0
        for date in Dates:
            LERow = m.LEDataRow(HeaderName, CorpID, date, Gas_Production[idx], Oil_Production[idx], Water_Production[idx], '')
            success, msg = LERow.Update(UpdateUser, datetime.now())
            if not success:
                msgs.append(msg)
                break
            else:
                msgs.append(CorpID + ' : ' + date + ' successfully updated in ' + HeaderName)

            idx = idx + 1 

    output = ConfigureOutput('', success, msgs)
    return output    

@app.route('/UpdateLEHeader', methods = ['POST'])
def CallUpdateLEHeader():
    """Interface Package Description"""
    interface = {
        "LEName" : str,
        "CorpID" : str,
        "ForecastGeneratedFrom" : str,
        "WellName" : str,
        "Wedge" : str,
        "LE_Date" : datetime,
        "UserName" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['LEName']
        CorpID = pkg['CorpID']
        ForecastGeneratedFrom = pkg['ForecastGeneratedFrom']
        WellName = pkg['WellName']
        Wedge = pkg['Wedge']
        LE_Date = pkg['LE_Date']
        UserName = pkg['UserName']

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)        

        LEHeaderRowObj = m.LEHeaderRow(LEName, WellName, CorpID, ForecastGeneratedFrom, Wedge, LE_Date, '')
        success, msg = LEHeaderRowObj.Update(UserName, datetime.now())

        output = ConfigureOutput('', success, msg)

    return output

@app.route('/UpdateForecast', methods=['POST'])
def CallUpdateForecastRow():
    """Interface Package Description"""
    interface = {
        "HeaderName" : str,
        "CorpID" : str,
        "Dates" : list,
        "Gas_Production" : list,
        "Oil_Production" : list,
        "Water_Production" : list,
        "Gas_NF" : list,
        "Oil_NF" : list,
        "Water_NF" : list,
        "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)
    msgs = []
    if success:
        HeaderName = pkg['HeaderName']
        CorpID = pkg['CorpID']
        Dates = pkg['Dates']
        Gas_Production = pkg['Gas_Production']
        Oil_Production = pkg['Oil_Production']
        Water_Production = pkg['Water_Production']
        GasNF = pkg['Gas_NF']
        OilNF = pkg['Oil_NF']
        WaterNF = pkg['Water_NF']
        UpdateUser = pkg['UpdateUser']

        idx = 0
        for date in Dates:
            ForecastRow = m.ForecastDataRow(HeaderName, CorpID, date, Gas_Production[idx], Oil_Production[idx], Water_Production[idx], GasNF[idx], OilNF[idx], WaterNF[idx], '')
            success, msg = ForecastRow.Update(UpdateUser, datetime.now())
            if not success:
                msgs.append(msg)
                break
            else:
                msgs.append(CorpID + ' : ' + date + ' successfully updated for ' + HeaderName)
            idx = idx + 1

    output = ConfigureOutput('', success, msgs)
    return output    

@app.route('/UpdateForecastHeader', methods=['POST'])
def CallUpdateForecastHeader():
    """Interface Package Description"""
    interface = {
        "HeaderName" : str,
        "WellName" : str,
        "CorpID" : str,
        "ForecastName" : str,
        "ForecastYear" : str,
        "scenarioName" : str,
        "GFO" : str,
        "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)
    
    if success:
        HeaderName = pkg['HeaderName']
        WellName = pkg['WellName']
        CorpID = pkg['CorpID']
        ForecastName = pkg['ForecastName']
        ForecastYear = pkg['ForecastYear']
        scenarioName = pkg['scenarioName']
        GFO = pkg['GFO']
        UserName = pkg['UpdateUser']

        Arps = {}
        Arps['Di'] = '' 
        Arps['qi'] = ''
        Arps['b'] = ''

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)

        ForecastHeaderRow = m.ForecastHeaderRow(WellName, CorpID, ForecastName, ForecastYear, scenarioName, Arps, GFO, '')
        success, msg = ForecastHeaderRow.Update(UserName, datetime.now())
    
    output = ConfigureOutput('', success, msg)


    return output

@app.route('/UpdateGasNF', methods=['POST'])
def CallUpdateGasNetting():
    """Interface Package Description"""
    interface = {
    "WellName" : str,
    "CorpID" : str,
    "NettingValue" : float,
    "NettingDate" : datetime,
    "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        WellName = pkg['WellName']
        CorpID = pkg['CorpID']
        NettingValue = pkg['NettingValue']
        NettingDate = pkg['NettingDate']
        UpdateUser = pkg['UpdateUser']

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)
      
        NettingRow = m.GasNettingRow(WellName, CorpID, NettingValue, NettingDate, '')
        success, msg = NettingRow.Update(UpdateUser, datetime.now())

    output = ConfigureOutput('', success, msg)
    return output    

@app.route('/UpdateOilNF', methods=['POST'])
def CallUpdateOilNetting():
    """Interface Package Description"""
    interface = {
    "WellName" : str,
    "CorpID" : str,
    "NettingValue" : float,
    "NettingDate" : datetime,
    "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        WellName = pkg['WellName']
        CorpID = pkg['CorpID']
        NettingValue = pkg['NettingValue']
        NettingDate = pkg['NettingDate']
        UpdateUser = pkg['UpdateUser']

        #Check CorpID if Wellname is passed and vice versa
        WellName, CorpID = iu.GetWellandCorpID(WellName, CorpID)
      
        NettingRow = m.GasNettingRow(WellName, CorpID, NettingValue, NettingDate, '')
        success, msg = NettingRow.Update(UpdateUser, datetime.now())

    output = ConfigureOutput('', success, msg)
    return output   

@app.route('/DeleteArea', methods=['DELETE'])
def CallDeleteArea():
    """Interface Package Description"""
    interface = {
    "AreaName" : str,
    "WellList" : list
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        AreaName = pkg['AreaName']
        WellList = pkg['WellList']
        AreaRowsObj = m.AreaAggregation('', [AreaName], WellList, [])
        rows, success, msg = AreaRowsObj.ReadTable()
        for row in rows:
            success, msg = row.Delete()

        if success:
            DBObj = row.DBObj
            DBObj.Command('commit')

    output = ConfigureOutput('', success, msg)
    return output    

@app.route('/UpdateFracHitMultipliers', methods=['POST'])
def CallUpdateFracHitMultipliers():
    """Interface Package Description"""
    interface = {
    "LEName" : str,
    "CorpID" : str,
    "Date_Key" : str,
    "Multiplier" : float,
    "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['LEName']
        CorpID = pkg['CorpID']
        Date_Key = pkg['Date_Key']
        Multiplier = pkg['Multiplier']
        UpdateUser = pkg['UpdateUser']

        FracHitRow = m.FracHitMultipliersRow(LEName, CorpID, Date_Key, Multiplier, '')
        success, msg = FracHitRow.Update(UpdateUser, datetime.now())

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/CreateAreaFromWells', methods=['POST'])
def CallCreateAreaFromWells():
    """Interface Package Description"""
    interface = {
        "AreaName" : str,
        "WellList" : list,
        "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        AreaName = pkg['AreaName']
        WellList = pkg['WellList']
        UpdateUser = pkg['UpdateUser']
        success, msg = di.CreateAreaFromWells(AreaName, WellList, UpdateUser)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/GetAreaDetails', methods=['GET'])
def CallGetAreaDetails():
    """Interface Package Description"""
    interface = {
    "AggregateName" : str,
    "WellNames" : list,
    "CorpIDs" : list
    }

    pkg, success, msg = InitializePayload(request, interface)

    rows = []
    if success:
        AggregateName = pkg['AggregateName']
        WellNames = pkg['WellNames']
        CorpIDs = pkg['CorpIDs']

        AggregateObj = m.AreaAggregation('', [AggregateName], WellNames, CorpIDs)
        rows, success, msg = AggregateObj.ReadTable()

    output = ConfigureOutput(rows, success, msg)

    return output

@app.route('/GetWedgeWells', methods=['GET'])
def CallGetWedge():
    """Interface Package Description"""
    interface = {
    "LEName" : str,
    "WedgeName" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        LEName = pkg['LEName']
        Wedge = pkg['WedgeName']

        #Get LE rows and match on passed Wedge
        LEHeaderObj = m.LEHeader('', [], [], [LEName], [])
        rows, success, msg = LEHeaderObj.ReadTable()

        ret_rows = []
        for row in rows:
            if row.Wedge == Wedge:
                ret_rows.append(row)

    output = ConfigureOutput(ret_rows, success, msg)
    return output

@app.route('/GetWells', methods=['GET'])
def CallGetWellsByLE():
    """Interface Package Description"""
    interface = {
    "LEName" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    rows = []
    if success:
        LEName = pkg['LEName']

        #Get LE rows
        LEHeaderObj = m.LEHeader('', [], [], [LEName], [])
        rows, success, msg = LEHeaderObj.ReadTable()

    output = ConfigureOutput(rows, success, msg)
    return output

@app.route('/FilterWellsByArea', methods=['GET'])
def CallFilterWellsByArea():
    """Interface Package Description"""
    interface = {
    "CorpIDList" : list,
    "Area" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    ret_well_list = []
    if success:
        CorpIDList = pkg['CorpIDList']
        Area = pkg['Area']

        AreaObj = m.AreaAggregation('', [Area], [], [])
        rows, success, msg = AreaObj.ReadTable()

        #Create area_well_list
        area_well_list = []
        for row in rows:
            area_well_list.append(row.CorpID)
        
        for corpID in CorpIDList:
            if corpID in area_well_list:
                ret_well_list.append(corpID)

    output = ConfigureOutput(ret_well_list, success, msg)
    return output

@app.route('/GetFracHitMultipliers', methods=['GET'])
def CallGetFracHitMultipliers():
    """Interface Package Description"""
    interface = {
    "LEName" : str,
    "CorpIDs" : list,
    "DateKeys" : list
    }

    pkg, success, msg = InitializePayload(request, interface)

    ros = []
    if success:
        LEName = pkg['LEName']
        CorpIDs = pkg['CorpIDs']
        DateKeys = pkg['DateKeys']

        FracHitObj = m.FracHitMultipliers('', [LEName], CorpIDs, DateKeys)
        rows, success, msg = FracHitObj.ReadTable()

    output = ConfigureOutput(rows, success, msg)
    return output

@app.route('/GetGasNF', methods=['GET'])
def CallGetGasNF():
    """Interface Package Description"""
    interface = {
    "WellNames" : list,
    "CorpIDs" : list
    }

    pkg, success, msg = InitializePayload(request, interface)
    rows = []
    if success:
        WellNames = pkg['WellNames']
        CorpIDs = pkg['CorpIDs']

        NFobj = m.GasNetting('', WellNames, CorpIDs)
        rows, success, msg = NFobj.ReadTable()

    output = ConfigureOutput(rows, success, msg)
    return output

@app.route('/GetOilNF', methods=['GET'])
def CallGetOilNF():
    """Interface Package Description"""
    interface = {
    "WellNames" : list,
    "CorpIDs" : list
    }

    pkg, success, msg = InitializePayload(request, interface)
    rows = []
    if success:
        WellNames = pkg['WellNames']
        CorpIDs = pkg['CorpIDs']

        NFobj = m.OilNetting('', WellNames, CorpIDs)
        rows, success, msg = NFobj.ReadTable()

    output = ConfigureOutput(rows, success, msg)
    return output

@app.route('/UploadForecastFromExcel', methods=['POST'])
def UploadForecastFromExcel():
    """Interface Package Description"""
    interface = {
    "FileName" : str,
    "SheetName" : str,
    "ForecastName" : str,
    "ForecastYear" : str,
    "GFO" : bool,
    "InterpolationMethod" : ['MonthlyVolumes', 'MonthlyRates', 'None'],
    "Phase" : ['Gas', 'Oil'],
    "DateRow" : int,    
    "DateStartColumn" : int,
    "DateEndColumn" : int,
    "StartRow" : int,
    "WellCol" : int,
    "CorpIDCol" : int,
    "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        FileName = pkg['FileName']
        SheetName = pkg['SheetName']
        ForecastName = pkg['ForecastName']
        ForecastYear = pkg['ForecastYear']
        Phase = pkg['Phase']
        GFO = pkg['GFO']
        InterpolationMethod = pkg['InterpolationMethod']
        DateRow = pkg['DateRow']
        DateStartColumn = pkg['DateStartColumn']
        DateEndColumn = pkg['DateEndColumn']
        StartRow = pkg['StartRow']
        WellCol = pkg['WellCol']
        CorpIDCol = pkg['CorpIDCol']
        UpdateUser = pkg['UpdateUser']
        
        success, msg = di.WriteForecastFromExcel(ForecastName, ForecastYear,'', GFO, FileName, SheetName, StartRow, CorpIDCol, WellCol, DateRow, DateStartColumn, DateEndColumn, InterpolationMethod, Phase, UpdateUser, datetime.now(), IDs = ['ALL'] )
        if not success:
            del_success, del_msg = le.DeleteForecast(ForecastName)

    output = ConfigureOutput('', success, msg)
    return output

@app.route('/UploadLEFromExcel', methods=['POST'])
def UploadLEFromExcel():
    """Interface Package Description"""
    interface = {
    "FileName" : str,
    "SheetName" : str,
    "LEName" : str,
    "LEDate" : datetime,
    "InterpolationMethod" : ['MonthlyVolumes', 'MonthlyRates', 'None'],
    "Phase" : ['Gas', 'Oil'],
    "DateRow" : int,    
    "DateStartColumn" : int,
    "DateEndColumn" : int,
    "StartRow" : int,
    "WellCol" : int,
    "CorpIDCol" : int,
    "UpdateUser" : str
    }

    pkg, success, msg = InitializePayload(request, interface)

    if success:
        FileName = pkg['FileName']
        SheetName = pkg['SheetName']
        LEName = pkg['LEName']
        LEDate = pkg['LEDate']
        Phase = pkg['Phase']
        InterpolationMethod = pkg['InterpolationMethod']
        DateRow = pkg['DateRow']
        DateStartColumn = pkg['DateStartColumn']
        DateEndColumn = pkg['DateEndColumn']
        StartRow = pkg['StartRow']
        WellCol = pkg['WellCol']
        CorpIDCol = pkg['CorpIDCol']
        UpdateUser = pkg['UpdateUser']

        success, msg = di.WriteLEFromExcel(LEName, LEDate, FileName, SheetName, StartRow, CorpIDCol, WellCol, DateRow, DateStartColumn, DateEndColumn, InterpolationMethod, Phase, UpdateUser, datetime.now(), IDs = ['ALL'] )

        if not success:
            #Rollback creation
            del_success, del_msg = le.DeleteLE(LEName)

    output = ConfigureOutput('', success, msg)
    return output

def InitializePayload(request, interface):
    pkg = js.loads(request.data)
    pkg, success, msg = ValidatePackage(pkg, interface)

    return pkg, success, msg

def ValidatePackage(pkg, interface):
    Success = True
    Message = ''

    key = ''
    value = ''
    try:
        for key, value in pkg.items():
            intf_type = interface[key]

            if intf_type == str:
                continue
            elif isinstance(intf_type, list):
                if value in intf_type:
                    continue
                else:
                    Success = False
                    # value_set = ''
                    # for item in intf_type:
                    #     value_set = value_set + str(item) 
                    Message = 'Passed value does not match set of valid values. Valid values -> ' + ', '.join(map(str, intf_type))
                    break
            elif intf_type == datetime:
                dt = pd.to_datetime(value)
                pkg[key] = dt

            elif intf_type == bool:
                valid_true_entries = ['TRUE', 'YES', '1']
                valid_false_entries = ['FALSE', 'NO', '0']
                if value.upper() in valid_true_entries:
                    pkg[key] = True
                    continue
                if value.upper() in valid_false_entries:
                    pkg[key] = False
                    continue
                else:
                    Message = 'Not a valid bool type: ' + value
                    break
                
            elif intf_type == list:
                if pkg[key]:
                    list_val = list(map(str.strip, list(value.split(','))))
                    pkg[key] = list_val
                else:
                    pkg[key] = []
            
            elif intf_type == int:
                if pkg[key]:
                    pkg[key] = int(value)
                else:
                    pkg[key] = ''

            elif intf_type == float:
                pkg[key] = float(value)
                
    except Exception as ex:
        Success = False
        Message = 'Error during validation of sent package. ' + key + ' : ' + str(value) + ' :: ' + str(ex)

    return pkg, Success, Message

def ConfigureOutput(ret_package, success, message):
    #A method that compiles return data (such as production data objects and messages) for the callers of the API

    output = {}
    output['Package'] = Serialize(ret_package)
    output['Success'] = success 
    output['Message'] = message

    return js.dumps(output, indent=4, sort_keys=True, default=str)

def Serialize(obj):

    if isinstance(obj, str):
        serialized = obj

    elif isinstance(obj, list):
        id = 0
        ser_dict = {}
        for item in obj:
            ser_dict[id] = Serialize(item)
            id = id + 1

        serialized = ser_dict

    elif isinstance(obj, pd.DataFrame):
        serialized = obj.to_dict()

    elif isinstance(obj, dict):
        serialized = obj

    elif isinstance(obj, object) and obj:
        serialized = obj.__dict__

    return serialized

if __name__ == '__main__':
    app.run()

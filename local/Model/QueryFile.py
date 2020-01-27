
 
def GetRoutes(area):
    query = 'select distinct Route from [dimensions].[wells] WITH (NOLOCK) where [Area] = \'' + area + '\''
    return query

def GetWells(area, route):
    query = 'select distinct WellName from [dimensions].[wells] WITH (NOLOCK) where [Route] = \'' + route + '\' and [Area] = \'' + area + '\''
    return query

def ScenarioQuery(scenario, corpID, start_date, end_date):
    start_date = StringifyDates(start_date)
    end_date = StringifyDates(end_date)

    if corpID[0] != 'ALL':
        in_clause = FormulateInClause(corpID, 'CORPORATE_ID')

        start_date = StringifyDates(start_date)
        end_date = StringifyDates(end_date)

        query = 'select S349, S370 as OilProduction, S376, S371 as GasProduction, C370, C371, C376, C753, C754, API10, API12, API14, WFLAC8 as WellFlac, wflac6, OUTDATE as Date, CORPORATE_ID as CorpID, m.LastUpdated'\
        ' from aries.ac_monthly m WITH (NOLOCK)' \
        ' left join aries.AC_PROPERTY p WITH (NOLOCK)'\
        ' on m.PROPNUM = p.PROPNUM '\
        ' left join aries.AC_ONELINE o WITH (NOLOCK)'\
        ' on m.scenario = o.scenario and m.PROPNUM = o.PROPNUM' + in_clause + ''\
        ' and m.scenario = \'' + scenario + '\' and OUTDATE >= \'' + start_date + '\' and OUTDATE <= \'' + end_date + '\''
    else:
        query = 'select S349, S370, S376, S371, C370 as OilProduction, C371 as GasProduction, C376, C753, C754, API10, API12, API14, WFLAC8 as WellFlac, wflac6, OUTDATE as Date, CORPORATE_ID as CorpID, m.LastUpdated'\
        ' from aries.ac_monthly m WITH (NOLOCK)' \
        ' left join aries.AC_PROPERTY p WITH (NOLOCK)'\
        ' on m.PROPNUM = p.PROPNUM'\
        ' left join aries.AC_ONELINE o WITH (NOLOCK)'\
        ' on m.scenario = o.scenario and m.PROPNUM = o.PROPNUM'\
        ' where m.scenario = \'' + scenario + '\' and OUTDATE >= \'' + start_date + '\' and OUTDATE <= \'' + end_date + '\''

    return query

def EDWKeyQueryFromCorpID(corpid_list, Area):
    in_clause = FormulateInClause(corpid_list, 'CorpID')

    query = 'select WellName, CorpID, API'\
        ' from [dimensions].[wells] WITH (NOLOCK) ' + in_clause

    if Area:
        query = query + ' and Area = \'' + Area + '\''

    return query

def EDWKeyQueryFromWellName(wellname_list):
    in_clause = FormulateInClause(wellname_list, 'WellName')

    query = 'select WellName, CorpID, API'\
        ' from [dimensions].[wells] WITH (NOLOCK) ' + in_clause

    return query

def EDWKeyQueryFromWellFlac(wellflac_list):
    in_clause = FormulateInClause(wellflac_list, 'Wellflac')

    query = 'select WellName, CorpID, Wellflac, API'\
        ' from [dimensions].[wells] WITH (NOLOCK) ' + in_clause

    return query

def EDWKeyQueryFromAPI(API_list):
    in_clause = FormulateInClause(API_list, 'API')

    query = 'select WellName, CorpID, API'\
        ' from [dimensions].[wells] WITH (NOLOCK) ' + in_clause


    return query


def GetActualsFromDB(corpid_list, start_date, end_date):
    in_clause = FormulateInClause(corpid_list, 'CorpID')

    query = 'select Oil'\
        ', Gas'\
        ', BOE'\
        ', Water'\
        ', MeasuredOil'\
        ', MeasuredGas'\
        ', MeasuredWater'\
        ', DateKey as Date_Key'\
        ', CorpID'\
        ' from [facts].[production] p WITH (NOLOCK) '\
        ' join [dimensions].[wells] w WITH (NOLOCK) '\
        ' on p.Wellkey = w.Wellkey ' + in_clause + ''\
        ' and DateKey >= ' + start_date + ' and DateKey <= ' + end_date

    return query

def GetGFOFromEastDB2019(WellName_FieldName, start_date, end_date):
    if WellName_FieldName[0] != 'ALL':
        in_clause = FormulateInClause(WellName_FieldName, 'i.[WellName_FieldName]')
    else:
        in_clause = ''

    query = 'select distinct gfo.[WellName_FieldName] as WellName'\
    ', [2019Zmcfd]'\
    ', [2019ZNF] as NettingFactor'\
    ', [Date]'\
    ' from [TeamOptimizationEngineering].[dbo].[GFOEast2019PlanTable] gfo WITH (NOLOCK)'\
    ' join [TeamOptimizationEngineering].[dbo].[GFOEastInputTable] i WITH (NOLOCK)'\
    ' on gfo.[WellName_FieldName] = i.[WellName_FieldName] ' + in_clause + ' and [Date] >= \'' + start_date + '\' and [Date] <= \'' + end_date + '\''

    return query

def GetGFOFromEastDB2018(WellName_FieldName, start_date, end_date):
    if WellName_FieldName[0] != 'ALL':
        in_clause = FormulateInClause(WellName_FieldName, 'i.[WellName_FieldName]')
    else:
        in_clause = ''

    query = 'select distinct gfo.[WellName_FieldName]'\
    ', [2018Zmcfd]'\
    ', [2018ZNF] as NettingFactor'\
    ', [Date]'\
    ' from [TeamOptimizationEngineering].[dbo].[GFOEast2018PlanTable] gfo WITH (NOLOCK)'\
    ' join [TeamOptimizationEngineering].[dbo].[GFOEastInputTable] i WITH (NOLOCK)'\
    ' on gfo.[WellName_FieldName] = i.[WellName_FieldName] ' + in_clause + ' and [Date] >= \'' + start_date + '\' and [Date] <= \'' + end_date + '\''

    return query

def GetNettingFactorsfromDB(wellname_list):

    query = 'select WellName, Wellflac, NF, NRI, FirstSalesDateInput '\
                'from [TeamOptimizationEngineering].[dbo].[GFOEastInputTable] ' \

    if wellname_list:
        in_clause = FormulateInClause(wellname_list, 'WellName')
        query =  query + ' ' + in_clause

    return query


def FormulateInClause(item_list, column_name):
    in_clause = 'where ' + str(column_name) + ' in ('
    count = 1
    for item in item_list:
        if count == len(item_list) and item:
            in_clause = in_clause + '\'' + str(item) + '\')'
        elif item:
            in_clause = in_clause + '\'' + str(item) + '\', '
        count = count + 1

    return in_clause

def ColumnQuery(table_name):
    query = 'select column_name from information_schema.columns where table_name = \'' + table_name + '\''

    return query

def RouteQuery(route_list):
    in_clause = FormulateInClause(route_list, 'route')

    query = 'select distinct name, apiNumber, wellflac from enbase.asset ' + in_clause + ' and apiNumber is not null and wellFlac is not null'

    return query

def FirstProductionDateQuery(corpid_list):
    in_clause = FormulateInClause(corpid_list, 'CorpID')

    query = 'select top 1 FirstProductionDate from Dimensions.Wells WITH (NOLOCK) ' + in_clause + ' and FirstProductionDate is not null order by FirstProductionDate Asc'

    return query


def GetActenumDrillScheduleData(start_date, end_date):
    import datetime

    query = '  select S_API as API,' \
    ' S_Name as WellName,' \
    ' N_LateralLength as LateralLength,' \
    ' N_ExpectedStages as ExpectedStages,' \
    ' T_StartFracWell as StartFracDate,' \
    ' T_FinishFracWell as EndFracDate,' \
    ' N_SurfaceLatitude as SurfaceLatitude,' \
    ' N_SurfaceLongitude as SurfaceLongitude,' \
    ' N_BottomHoleLatitude as BottomHoleLatitude,' \
    ' N_BottomHoleLongitude as BottomHoleLongitude,' \
    ' framework_valid_from as UpdateDate' \
    ' from [bpx_actenum].[wells.asis] a' \
    ' where T_StartFracWell > \'' + start_date + '\' and T_StartFracWell < \'' + end_date + '\'' \
    ' and N_SurfaceLatitude is not null' \
    ' and N_SurfaceLongitude is not null' \
    ' and N_BottomHoleLatitude is not null' \
    ' and N_BottomHoleLongitude is not null' \
    ' and DELETED = 0' \
    ' and S_API is not null' \
    ' and S_API <> \'\''

    return query

def GetWellsWithinBearing(lat, lon, distance):
    str_lat = str(lat)
    str_long = str(lon)
    str_dist = str(distance)

    query = 'select [WELL_NAME]'\
        ', [UWI]'\
        ', [SURFACE_LATITUDE] as SurfaceLatitude'\
        ', [SURFACE_LONGITUDE] as SurfaceLongitude'\
        ', [BOTTOM_HOLE_LATITUDE] as BottomHoleLatitude'\
        ', [BOTTOM_HOLE_LONGITUDE] as BottomHoleLongitude'\
        ', e_var.DISTANCE'\
        ' FROM [bpx_tdm].[WELL.asis]'\
        ' cross apply (select (RADIANS(([SURFACE_LONGITUDE]) - ('+str_long+'))) as dlon) as a_var'\
        ' cross apply (select (RADIANS(([SURFACE_LATITUDE]) - ('+str_lat+'))) as dlat) as b_var'\
        ' cross apply (select (POWER(sin(dlat/2), 2) + cos(RADIANS('+str_lat+')) * cos(RADIANS([SURFACE_LATITUDE])) * POWER(sin(dlon/2), 2)) as a) as c_var'\
        ' cross apply (select (2 * atn2( sqrt(a), sqrt(1-a) )) as c) as d_var'\
        ' cross apply (select (3958.8 * 5280 * c) as DISTANCE) as e_var'\
        ' where e_var.DISTANCE < ' + str_dist + ''\
        ' and [SURFACE_LATITUDE] is not null'\
        ' and [SURFACE_LONGITUDE] is not null'\
        ' and [BOTTOM_HOLE_LATITUDE] is not null'\
        ' and [BOTTOM_HOLE_LATITUDE] is not null'

    return query

def StringifyDates(date):
    import pandas as pd

    if not isinstance(date, str):
        date = pd.to_datetime(date)
        date = date.strftime('%m/%d/%Y')

    return date

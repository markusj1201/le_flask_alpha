"""
***************************************************************************************
   Description: This module is designed to perform calculations that affect production 
   due to frac hit mitagation operational shut-ins.
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


def FracHatMitigation(LEName, EastWestFracHitRadius, NorthSouthFracHitRadius, Update_User):
    from Model import ModelLayer as m
    from Model import QueryFile as qf
    from Model import BPXDatabase as bpx
    import pandas as pd
    from datetime import datetime, timedelta
    import numpy as np

    Success = True
    Messages = []
    try:
        #Find Frac Date of Upcoming Wells

        #Get the beginning and end date of the LE being evaluated
        LEDataObj = m.LEData('', [LEName], [], [])
        LErows, Success, Message = LEDataObj.ReadTable()

        if not Success:
            Messages.append(Message)
        elif len(LErows) < 1:
            Messages.append('No LE Data exists for the given LE Name.')
        else:
            LErows_df = pd.DataFrame([vars(s) for s in LErows])
            start_date = LErows_df['Date_Key'].min()
            end_date = LErows_df['Date_Key'].max()

            s_start_date = datetime.strftime(start_date, '%m/%d/%Y')
            s_end_date = datetime.strftime(end_date, '%m/%d/%Y' )

            #Query the Drill Schedule (and potentially other data sources) for upcoming drills

            new_drill_query = qf.GetActenumDrillScheduleData(s_start_date, s_end_date)
            DBObj = bpx.GetDBEnvironment('ProdEDH', 'OVERRIDE')
            dso_results = DBObj.Query(new_drill_query)
            if not Success:
                Messages.append(Message)

            else:
                dso_df = dso_results[1]

                for nd_idx, nd_row in dso_df.iterrows():
                    #Get Lateral and Longitude values
                    surface_lat = nd_row['SurfaceLatitude']
                    surface_long = nd_row['SurfaceLongitude']
                    if surface_long > 0:
                        surface_long = 0 - surface_long
                    bh_lat = nd_row['BottomHoleLatitude']
                    bh_long = nd_row['BottomHoleLongitude']
                    if bh_long > 0:
                        bh_long = 0 - bh_long
                    stages = nd_row['ExpectedStages']
                    name = nd_row['WellName']
                    frac_start = nd_row['StartFracDate']
                    frac_end = nd_row['EndFracDate']

                    #Get wells within certain distance
                    FracHitRadius = max(EastWestFracHitRadius, NorthSouthFracHitRadius)
                    from_surface_query = qf.GetWellsWithinBearing(surface_lat, surface_long, FracHitRadius)
                    from_bottom_query = qf.GetWellsWithinBearing(bh_lat, bh_long, FracHitRadius)

                    surface_res = DBObj.Query(from_surface_query)
                    bh_res  = DBObj.Query(from_bottom_query)

                    if not surface_res[1].empty:
                        all_res = surface_res[1]
                        if not bh_res[1].empty:
                            all_res = pd.merge(surface_res[1], bh_res[1])
                    elif not bh_res[1].empty:
                        all_res = bh_res[1]
                    else:
                        all_res = pd.DataFrame()

                    stages = int(stages)

                    #Get interpolated bearings
                    interpolated_bearings = InterpolateBearing([bh_lat, bh_long], [surface_lat, surface_long], stages) #reversed since frac will begin at bottom hole location
                    interpolated_dates = InterpolateDates(frac_start, frac_end, stages)
                    interp = np.column_stack([interpolated_bearings, interpolated_dates])

                    if all_res.empty:
                        Messages.append('No wells within given frac hit radius from ' + name + ' to apply multipliers.')

                    #Loop through all the well results
                    for ex_idx, ex_row in all_res.iterrows():
                        corpid = ex_row['UWI']
                        wellname = ex_row['WELL_NAME']
                        dates = []                    
                        ex_surface_lat = float(ex_row['SurfaceLatitude'])
                        ex_surface_long = float(ex_row['SurfaceLongitude'])
                        if ex_surface_long > 0:
                            ex_surface_long = 0 - ex_surface_long
                        ex_bh_lat = float(ex_row['BottomHoleLatitude'])
                        ex_bh_long = float(ex_row['BottomHoleLongitude'])
                        if ex_bh_long > 0:
                            ex_bh_long = 0 - ex_bh_long

                        ex_interp_bearings = InterpolateBearing([ex_surface_lat, ex_surface_long], [ex_bh_lat, ex_bh_long], stages)

                        # cycle through each item in the interpolated array and calculate distance to points in existing well
                        for point in interp:
                            for bearing in ex_interp_bearings:
                                distance = CalculateDistanceFromBearings([point[0], point[1]], bearing)
                                azimuth = CalculateAzimuthFromBearings([point[0], point[1]], bearing)

                                if (azimuth >= 25 and azimuth <= 155) or (azimuth >= 205 and azimuth <= 335):
                                    #Evaluate against EastWestFrac radius
                                    if distance < EastWestFracHitRadius:
                                        dates.append(point[2])
                                else:
                                    if distance < NorthSouthFracHitRadius:
                                        dates.append(point[2])

                        dates = list(dict.fromkeys(dates))
                        #Go through the dates and add the ramp up, ramp up schedule dates and multipliers
                        if len(dates) > 0:
                            ramp_up = [1, 1, 0.5]
                            ramp_down = [0.5, 0.75, 1]

                            min_date = min(dates)
                            max_date = max(dates)
                            ramp_down_dates = [(min_date - timedelta(days = 3)), (min_date - timedelta(days = 2)), (min_date - timedelta(days = 1))]
                            ramp_up_dates =[(max_date + timedelta(days = 3)), (max_date + timedelta(days = 2)), (max_date + timedelta(days = 1))]

                            shut_ins = [0] * len(dates)
                            all_multipliers = []
                            all_multipliers.extend(ramp_up)
                            all_multipliers.extend(shut_ins)
                            all_multipliers.extend(ramp_down)

                            all_dates = []
                            all_dates.extend(ramp_up_dates)
                            all_dates.extend(dates)
                            all_dates.extend(ramp_down_dates)

                            combined_date_multiplier = np.column_stack([all_dates, all_multipliers])

                            for date_multiplier in combined_date_multiplier:
                                #Search DB for frac hit multiplier row
                                date = datetime.strftime(date_multiplier[0], '%m/%d/%Y')
                                fhm_obj  = m.FracHitMultipliers('', [LEName], [corpid], [date])
                                fhm_rows, Success, Message = fhm_obj.ReadTable()

                                if Success and len(fhm_rows) > 0:
                                    #if the row exists, then update
                                    row = fhm_rows[0]
                                    row.Multiplier = date_multiplier[1]
                                    Success, Message = row.Update(Update_User, datetime.now())
                                    if not Success:
                                        Messages.extend(Message)
                                elif Success and len(fhm_rows) == 0:
                                    Messages.append('No entry exists for ' + wellname + ' on ' + date + ' for LE ' + LEName + '. ')
                                else:   
                                    Messages.extend(Message)
                
    
    except Exception as ex:
        Success = False
        Messages.append('Error during the automatic application of frac hit mitigation multipliers. ' + str(ex))

    return Success, Messages



def InterpolateBearing(start_latlong, end_latlong, segment_num):
    #Input a set of lat long coordinates at start and end
    import numpy as np

    lat_lin = np.linspace(start_latlong[0], end_latlong[0], segment_num)
    long_lin = np.linspace(start_latlong[1], end_latlong[1], segment_num)

    interpolated_bearings = []
    for i in range(segment_num):
        bearing = [lat_lin[i], long_lin[i]]
        interpolated_bearings.append(bearing)

    return interpolated_bearings

def InterpolateDates(start_date, end_date, segment_num):
    import pandas as pd
    from datetime import datetime as dt
    from datetime import timedelta
    import numpy as np

    interpolated_dates = []
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    #Convert start_date, end_date to series
    delta = end_date - start_date

    total_days = delta.days
    day_lin = np.linspace(0, total_days, segment_num)

    for item in day_lin:
        interpolated_dates.append(dt.date(start_date + timedelta(days = item)))

    return interpolated_dates


def CalculateDistanceFromBearings(start_bearing, end_bearing):
    import numpy as np
    import math

    r_start_lat = math.radians(start_bearing[0])
    r_end_lat = math.radians(end_bearing[0])
    dlon = math.radians(end_bearing[1] - start_bearing[1])
    dlat = math.radians(end_bearing[0] - start_bearing[0])
    a = (math.sin(dlat/2))**2 + math.cos(r_end_lat) * math.cos(r_start_lat) * (math.sin(dlon/2))**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    radius = 3958.8 * 5280 #mile radius of earth into feet
    distance = radius * c

    return distance

def CalculateAzimuthFromBearings(start_bearing, end_bearing):
    import math
    import numpy as np

    azimuth = 0
    lat1 = start_bearing[0]
    lat2 = end_bearing[0]

    long1 = start_bearing[1]
    long2 = end_bearing[1]

    dlat = lat2-lat1
    dlong = long2-long1

    azimuth = math.atan2((math.sin(dlong) * math.cos(lat2)), (math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlong)))

    azimuth = math.degrees(azimuth) + 360
    
    return azimuth

if __name__ == '__main__':
    FracHitRadius = 5000
    Success, Messages = FracHatMitigation('Test_Create_Total_July', 5000, 500, 'Travis Comer')
    # interp_dates = InterpolateDates('01/01/2019', '01/15/2019', 45)

    print(Success)


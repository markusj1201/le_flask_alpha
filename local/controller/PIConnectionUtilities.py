"""
***************************************************************************************
   Description: A class/set of methods that simplify the connection to PI systems.
   Include the AF SDK in the same directory as this utility in order to use.
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
   1.0        Travis C    07/23/2019 Initial Creation
***************************************************************************************
"""
import sys  
import clr  

clr.AddReference('OSIsoft.AFSDK')  

from OSIsoft.AF import *  
from OSIsoft.AF.PI import *
from OSIsoft.AF.Data import *
from OSIsoft.AF.Time import *  
from OSIsoft.AF.UnitsOfMeasure import *

def ConnectToPIServer(servername):
    Success = True
    Messages = ''

    piServer = []
    try:
        piServers = PIServers()
        if not servername:
            piServer = piServers.DefaultPIServer
        else:
            piServer = piServers[servername]  

    except Exception as ex:
        Messages = str(ex)
        Success = False
    
    return piServer, Success, Messages


def GetPIPoints(server, PiPointName, start_timerange, end_timerange):
    Success = True
    Messages = ''
    
    try:
        #Get Server
        piServer, Success, Messages = ConnectToPIServer(server)
        if Success:
            #Must be input via PI Time notation (ex: '*-3h' for a start time of 3 hours ago)
            timerange = AFTimeRange(start_timerange, end_timerange)
            piPoint = PIPoint.FindPIPoint(piServer, PiPointName)
            recorded_values = piPoint.RecordedValues(timerange, AFBoundaryType.Inside, "", False)

    except Exception as ex:
        Messages = str(ex)
        Success = False
        recorded_values = []
    
    return recorded_values, Success, Messages

def ReturnPIPoint(piServerName, PiPointName):
    Success = False
    Messages = ''
    piPoint = []
    try:
        piServer, Success, Messages = ConnectToPIServer(piServerName)
        if Success:
            piPoint = PIPoint.FindPIPoint(piServer, PiPointName)
    except Exception as ex:
        Success = False
        Messages = str(ex)

    return piPoint, Success, Messages

def GetInterpolatedPIPoints(server, PiPointName, start_timerange, end_timerange, span):
    Success = True
    Messages = ''
    
    try:
        #Get Server
        piServer, Success, Messages = ConnectToPIServer(server)
        
        if Success:
            #Must be input via PI Time notation (ex: '*-3h' for a start time of 3 hours ago)
            timerange = AFTimeRange(start_timerange, end_timerange)
            span = AFTimeSpan.Parse(span)
            
            piPoint = PIPoint.FindPIPoint(piServer, PiPointName)
            recorded_values = piPoint.InterpolatedValues(timerange, span, "", False)

    except Exception as ex:
        Messages = str(ex)
        Success = False
        recorded_values = []
    
    return recorded_values, Success, Messages

def PlotPIPoints(PiPoint, start_time, end_time):
    Success = True
    Messages = ''

    try:
        timerange = AFTimeRange(start_time, end_time)
        plotValues = PiPoint.PlotValues(timerange, 1)

    except Exception as ex:
        Messages = str(ex)
        Success = False

    return plotValues, Success, Messages


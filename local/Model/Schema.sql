CREATE TABLE LE_Header (
    LEName varchar(50) NOT NULL,
    WellName varchar(50) NOT NULL,
    CorpID varchar(25) NOT NULL,
	ForecastGeneratedFrom varchar(50) NOT NULL,
    Wedge varchar(35) NOT NULL,
	LE_Date datetime NOT NULL,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
    PRIMARY KEY (LEName, CorpID, LE_Date)
);


Create Table LE_Data (
	HeaderName varchar(50) NOT NULL,
	CorpID varchar(25) NOT NULL,
	Date_Key datetime NOT NULL,
	Gas_Production real NOT NULL,
	Oil_Production real,
	Water_Production real,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY(HeaderName, CorpID, Date_Key)
);

create table Forecast_Header (
	WellName varchar(25) NOT NULL,
	CorpID varchar(25) NOT NULL,
	ForecastName varchar(50) NOT NULL,
	GFOz bit default 0,
	GFOzYear datetime,
	Aries_ScenarioID varchar(100),
	Update_Date datetime NOT NULL, 
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (ForecastName, CorpID)
);

create table Forecast_Data (
	HeaderName varchar(50) NOT NULL,
	CorpID varchar(50) NOT NULL,
	Date_Key datetime NOT NULL,
	Gas_Production real,
	Oil_Production real,
	Water_Production real,
	GasNettingFactor real, 
	OilNettingFactor real, 
	WaterNettingFactor real, 
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (HeaderName, CorpID, Date_Key)
);

create table LE_Summary (
	SummaryName varchar(50) NOT NULL,
	Wedge varchar(20),
	Midstream varchar(100),
	Reason varchar(255),
	Comments varchar(255),
	SummaryDate datetime,
	LEName varchar(50),
	GFOForecastName varchar(50),
	MonthlyAvgMBOED real,
	QuarterlyAvgMBOED real,
	AnnualAvgMBOED real,
	MonthlyGFOMBOED real,
	QuarterlyGFOMBOED real,
	AnnualGFOMBOED real,
	MonthlyVariance real,
	QuarterlyVariance real,
	AnnualVariance real,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (SummaryName, Wedge)
);


create table GasNettingValues (
	WellName varchar(50),
	CorpID varchar(20),
	NettingValue real,
	NettingDate datetime,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (CorpID, NettingDate)
);

create table OilNettingValues (
	WellName varchar(50),
	CorpID varchar(20),
	NettingValue real,
	NettingDate datetime,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (CorpID, NettingDate)
);

create table Frac_Hit_Multipliers (
	LEName varchar(50) NOT NULL,
	CorpID varchar(20) NOT NULL,
	Date_Key datetime NOT NULL,
	Multiplier float,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (LEName, CorpID, Date_Key)
);

create table AreaAggregation (
	AggregateName varchar(50) NOT NULL,
	WellName varchar(50),
	CorpID varchar(50) NOT NULL,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY (AggregateName, CorpID)
);

create table ProductionAdjustments (
	LEName varchar(50) NOT NULL,
	WellName varchar(50),
	CorpID varchar (20) NOT NULL,
	AdjustedGasProduction real,
	AdjustedOilProduction real,
	AdjustedWaterProduction real,
	Date_Key datetime NOT NULL,
	Update_Date datetime NOT NULL,
	Update_User varchar(50) NOT NULL
	PRIMARY KEY(LEName, CorpID, Date_Key)
);

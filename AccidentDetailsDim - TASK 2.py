import pandas as pd
import os
import pyodbc
import urllib
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time, VARCHAR, Identity
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np

# Define the connection strings to the databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_db)
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUKDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_dw)

Base = declarative_base()

class AccidentDim(Base):
    __tablename__ = 'AccidentDetailsDim'
    Accident_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
    Accident_Index = Column(String(500))
    Accident_Severity = Column(String(255))
    Carriageway_Hazards = Column(String(255), default=None)
    Did_Police_Officer_Attend_Scene_of_Accident = Column(Integer)
    Junction_Control = Column(String(255))
    Junction_Detail = Column(String(255))
    Latitude = Column(Float)
    Light_Conditions = Column(String(255))
    Local_Authority_District = Column(String(255))
    Local_Authority_Highway = Column(String(255))
    Location_Easting_OSGR = Column(Integer)
    Location_Northing_OSGR = Column(Integer)
    Longitude = Column(Float)
    LSOA_of_Accident_Location = Column(String(255))
    Pedestrian_Crossing_Human_Control = Column(Integer)
    Pedestrian_Crossing_Physical_Facilities = Column(Integer)
    Police_Force = Column(String(255))
    Special_Conditions_at_Site = Column(String(255), default=None)
    Urban_or_Rural_Area = Column(String(255))
    Weather_Conditions = Column(String(255))
    Year = Column(Integer)
    InScotland = Column(String(255), default=None)

def pipe_Customer(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Accident_Details;'  # Make sure this matches your actual data source
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Replace np.inf with np.nan and fill nan with 0
    dFrame.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    dFrame.fillna(0, inplace=True)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        new_record = AccidentDim(
            Accident_Index=row['Accident_Index'],
            Accident_Severity=row['Accident_Severity'],
            Carriageway_Hazards=row['Carriageway_Hazards'],
            Did_Police_Officer_Attend_Scene_of_Accident=row['Did_Police_Officer_Attend_Scene_of_Accident'],
            Junction_Control=row['Junction_Control'],
            Junction_Detail=row['Junction_Detail'],
            Light_Conditions=row['Light_Conditions'],
            Local_Authority_District=row['Local_Authority_District'],
            Local_Authority_Highway=row['Local_Authority_Highway'],
            Location_Easting_OSGR=row['Location_Easting_OSGR'],
            Location_Northing_OSGR=row['Location_Northing_OSGR'],
            Latitude=row['Latitude'],
            Longitude=row['Longitude'],
            LSOA_of_Accident_Location=row['LSOA_of_Accident_Location'],
            Pedestrian_Crossing_Human_Control=row['Pedestrian_Crossing_Human_Control'],
            Pedestrian_Crossing_Physical_Facilities=row['Pedestrian_Crossing_Physical_Facilities'],
            Police_Force=row['Police_Force'],
            Special_Conditions_at_Site=row['Special_Conditions_at_Site'],
            Urban_or_Rural_Area=row['Urban_or_Rural_Area'],
            Weather_Conditions=row['Weather_Conditions'],
            Year=row['Year'],
            InScotland=row['InScotland']
        )
        session.add(new_record)
        counter += 1

    session.commit()
    session.close()

    # Logging
    log_activity(counter)

def log_activity(records_loaded):
    with open('accident_details_Dim_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = "TimeStamp: {} --- Number of records loaded into AccidentDetailsDim: {}\n".format(current_time, records_loaded)
        log_file.write(log_message)

pipe_Customer(conn_string_db, conn_string_dw)

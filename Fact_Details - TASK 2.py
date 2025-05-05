import pandas as pd
import os
import pyodbc
import urllib
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time, VARCHAR, Identity, ForeignKey,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker,relationship
import numpy as np

# Define the connection strings to the databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_db)
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUKDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_dw)

# Base = declarative_base()

# class AccidentDim(Base):
#     __tablename__ = 'AccidentDetailsDim'
#     Accident_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
#     Accident_Index = Column(String(500))
#     Accident_Severity = Column(String(255))
#     Carriageway_Hazards = Column(String(255), default=None)
#     Did_Police_Officer_Attend_Scene_of_Accident = Column(Integer)
#     Junction_Control = Column(String(255))
#     Junction_Detail = Column(String(255))
#     Latitude = Column(Float)
#     Light_Conditions = Column(String(255))
#     Local_Authority_District = Column(String(255))
#     Local_Authority_Highway = Column(String(255))
#     Location_Easting_OSGR = Column(Integer)
#     Location_Northing_OSGR = Column(Integer)
#     Longitude = Column(Float)
#     LSOA_of_Accident_Location = Column(String(255))
#     Pedestrian_Crossing_Human_Control = Column(Integer)
#     Pedestrian_Crossing_Physical_Facilities = Column(Integer)
#     Police_Force = Column(String(255))
#     Special_Conditions_at_Site = Column(String(255), default=None)
#     Urban_or_Rural_Area = Column(String(255))
#     Weather_Conditions = Column(String(255))
#     Year = Column(Integer)
#     InScotland = Column(String(255), default=None)

concatenated_df = []
def pipe_Customer(conn_string_db, conn_string_dw):
    engine_db = create_engine(conn_string_db)
    engine_dw = create_engine(conn_string_dw)

    sqlQuery1 = f'select Number_of_Casualties, Number_of_Vehicles, Speed_limit from Accident_Details;'
    df1 = pd.read_sql_query(sqlQuery1, engine_db)
    # print(df1)

    sqlQuery2 = f'select Age_of_Vehicle, Driver_IMD_Decile, Engine_Capacity_CC from Vehicle_Details;'
    df2 = pd.read_sql_query(sqlQuery2, engine_db)
    #print(df2)

    sqlQuery3 = f'select Date_ID from AccidentTimeDim;'
    df3 = pd.read_sql_query(sqlQuery3, engine_dw)
    # print(df3)

    sqlQuery4 = f'select Road_ID from AccidentRoadDetailsDim;'
    df4 = pd.read_sql_query(sqlQuery4, engine_dw)

    sqlQuery5 = f'select Accident_ID, Accident_Index from AccidentDetailsDim;'
    df5 = pd.read_sql_query(sqlQuery5, engine_dw)

    sqlQuery6 = f'select Vehicle_ID from VehicleDetailsDim;'
    df6 = pd.read_sql_query(sqlQuery6, engine_dw)

    concatenated_df = pd.concat([df1, df2, df3, df4, df5, df6], axis=1)
    #data_types = concatenated_df.dtypes


    Base = declarative_base()

    class acc_det(Base):
        __tablename__ = "AccidentDetailsDim"
        Accident_ID = Column(Integer, primary_key=True)

    class acc_time(Base):
        __tablename__ = "AccidentTimeDim"
        Date_ID = Column(Integer, primary_key=True)

    class road_detail(Base):
        __tablename__ = "AccidentRoadDetailsDim"
        Road_ID = Column(Integer, primary_key=True)

    class veh_Detail(Base):
        __tablename__ = "VehicleDetailsDim"
        Vehicle_ID = Column(Integer, primary_key=True)


    class Fact_Details(Base):
        __tablename__ = 'Fact_Details'
        Fact_ID = Column(Integer, primary_key=True, autoincrement=True)
        Accident_Index = Column(String(255))
        Accident_ID = Column(Integer, ForeignKey('AccidentDetailsDim.Accident_ID'))
        Date_ID = Column(Integer, ForeignKey('AccidentTimeDim.Date_ID'))
        Road_ID = Column(Integer, ForeignKey('AccidentRoadDetailsDim.Road_ID'))
        Vehicle_ID = Column(Integer, ForeignKey('VehicleDetailsDim.Vehicle_ID'))
        Number_of_Casualties = Column(Integer)
        Number_of_Vehicles = Column(Integer)
        Speed_limit = Column(Integer)
        Age_of_Vehicle = Column(Integer)
        Driver_IMD_Decile = Column(Integer)
        Engine_Capacity_CC = Column(Integer)

    Relatn_Acc_Fact = relationship("AccidentDetailsDim")
    Relatn_Time_Fact = relationship("AccidentTimeDim")
    Relatn_Road_Fact = relationship("AccidentRoadDetailsDim")
    Relatn_Vehicle_Fact = relationship("VehicleDetailsDim")

    engine_dw = create_engine(conn_string_dw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter=0

    for _, row in concatenated_df .iterrows():

        nullcheck = {key: None if pd.isna(value) else value for key, value in row.to_dict().items()}

        new_record = Fact_Details(**nullcheck)
        session.add(new_record)
        counter += 1
        print("Data entered", counter)

        session.commit()
    session.close()
    log_activity(counter)

def log_activity(message):
    with open('Fact_Details.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into Fact Details: {message}\n"
        log_file.write(log_message)


pipe_Customer(conn_string_db, conn_string_dw)


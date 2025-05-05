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

class VehicleDim(Base):
    __tablename__ = 'VehicleDetailsDim'
    Vehicle_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
    Hit_Object_in_Carriageway = Column(String(255))
    Hit_Object_off_Carriageway = Column(String(255))
    Journey_Purpose_of_Driver = Column(String(255))
    Junction_Location = Column(String(255))
    make = Column(String(255))
    model = Column(String(255))
    Skidding_and_Overturning = Column(String(255))
    Towing_and_Articulation = Column(String(255))
    Vehicle_Leaving_Carriageway = Column(String(255))
    Vehicle_Location_Restricted_Lane = Column(String(255))
    Vehicle_Manoeuvre = Column(String(255))
    Vehicle_Type = Column(String(255))
    Was_Vehicle_Left_Hand_Drive = Column(String(255))
    X1st_Point_of_Impact = Column(String(255))
    Age_Band_of_Driver = Column(String(255))
    Driver_Home_Area_Type = Column(String(255))
    Propulsion_Code = Column(String(255))
    Sex_of_Driver = Column(String(255))
    Vehicle_Reference = Column(Integer)

def pipe_vehicle_details(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Vehicle_Details;'  # Modify query as needed to fit your data structure
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    # Replace np.inf with np.nan and fill nan with 0
    dFrame.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    dFrame.fillna(0, inplace=True)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        new_record = VehicleDim(
            Hit_Object_in_Carriageway=row['Hit_Object_in_Carriageway'],
            Hit_Object_off_Carriageway=row['Hit_Object_off_Carriageway'],
            Journey_Purpose_of_Driver=row['Journey_Purpose_of_Driver'],
            Junction_Location=row['Junction_Location'],
            make=row['make'],
            model=row['model'],
            Skidding_and_Overturning=row['Skidding_and_Overturning'],
            Towing_and_Articulation=row['Towing_and_Articulation'],
            Vehicle_Leaving_Carriageway=row['Vehicle_Leaving_Carriageway'],
            Vehicle_Location_Restricted_Lane=row['Vehicle_Location_Restricted_Lane'],
            Vehicle_Manoeuvre=row['Vehicle_Manoeuvre'],
            Vehicle_Type=row['Vehicle_Type'],
            Was_Vehicle_Left_Hand_Drive=row['Was_Vehicle_Left_Hand_Drive'],
            X1st_Point_of_Impact=row['X1st_Point_of_Impact'],
            Age_Band_of_Driver=row['Age_Band_of_Driver'],
            Driver_Home_Area_Type=row['Driver_Home_Area_Type'],
            Propulsion_Code=row['Propulsion_Code'],
            Sex_of_Driver=row['Sex_of_Driver'],
            Vehicle_Reference=row['Vehicle_Reference']
        )

        session.add(new_record)
        counter += 1

    session.commit()
    session.close()

    # Logging
    log_activity(counter)

def log_activity(records_loaded):
    with open('vehicle_details_Dim_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = "TimeStamp: {} --- Number of records loaded into vehicleDetailsDim: {}\n".format(current_time, records_loaded)
        log_file.write(log_message)


pipe_vehicle_details(conn_string_db, conn_string_dw)

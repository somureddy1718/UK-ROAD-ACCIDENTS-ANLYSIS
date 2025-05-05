import pandas as pd
import os
import pyodbc
import urllib
import datetime
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Identity,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True
#
# Define the connection strings to the MyStore and MyStoreDWETL databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db

#####################################################################################

#
#
#   This is ORM whereby an object is created corresponding to the database table DimCustomers
Base = declarative_base()

#   The ORM DimCustomers object bound to the database table DimCustomers
class AccidentDetails(Base):
    __tablename__ = 'Accident_Details'
    Accident_ID = Column(Integer, primary_key=True)
    Accident_Index = Column(String(500))


class VehicleDetails(Base):
    __tablename__ = 'Vehicle_Details'
    Vehicle_ID = Column(Integer, primary_key=True)
    Accident_ID = Column(Integer, ForeignKey('Accident_Details.Accident_ID'))
    Accident_Index = Column(String(500))
    Hit_Object_in_Carriageway = Column(String(255))
    Hit_Object_off_Carriageway = Column(String(255))
    Journey_Purpose_of_Driver = Column(String(255))
    Junction_Location = Column(String(255))
    make = Column(String(255))
    model = Column(String(255))
    Skidding_and_Overturning = Column(String(255))
    Towing_and_Articulation = Column(String(255))
    Vehicle_Leaving_Carriageway = Column(String(255))
    Vehicle_Location_Restricted_Lane = Column(Integer)
    Vehicle_Manoeuvre = Column(String(255))
    Vehicle_Type = Column(String(255))
    Was_Vehicle_Left_Hand_Drive = Column(String(255))
    X1st_Point_of_Impact = Column(String(255))
    Age_Band_of_Driver = Column(String(255))
    Age_of_Vehicle = Column(Integer)
    Driver_Home_Area_Type = Column(String(255))
    Driver_IMD_Decile = Column(Integer)
    Engine_Capacity_CC = Column(Integer)
    Propulsion_Code = Column(String(255))
    Sex_of_Driver = Column(String(255))
    Vehicle_Reference = Column(Integer)



# Create an ORM session connected to the dimensional database MyStoreDWETL using the connection-string conStrdw,
def convert_nan_and_empty_to_none(value):
    if pd.isna(value) or value == '':
        return None
    else:
        return value
def load_vehicle_details(csv_path,connection_string):
    engine= create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session =Session()
    counter = 0
    df=pd.read_csv(csv_path)
    df = df[['Accident_Index', 'Hit_Object_in_Carriageway', 'Hit_Object_off_Carriageway', 'Journey_Purpose_of_Driver',
             'Junction_Location', 'make', 'model', 'Skidding_and_Overturning', 'Towing_and_Articulation',
             'Vehicle_Leaving_Carriageway', 'Vehicle_Location.Restricted_Lane', 'Vehicle_Manoeuvre',
             'Vehicle_Type', 'Was_Vehicle_Left_Hand_Drive', 'X1st_Point_of_Impact', 'Age_Band_of_Driver',
             'Age_of_Vehicle', 'Driver_IMD_Decile', 'Engine_Capacity_CC', 'Propulsion_Code', 'Sex_of_Driver',
             'Vehicle_Reference','Driver_Home_Area_Type']]


    for value, row in df.iterrows():

        if row['Was_Vehicle_Left_Hand_Drive'] not in ('No', 'Yes', 'Data missing or out of range'):
            log_message(f"Invalid Was_Vehicle_Left_Hand_Drive data in the csv file at {value}")
            continue


        vehicle_details_obj = session.query(AccidentDetails).filter_by(
            Accident_Index=str(row['Accident_Index'])).first()

        if vehicle_details_obj:
            HCOIN = convert_nan_and_empty_to_none(row['Hit_Object_in_Carriageway'])
            HCOOUT = convert_nan_and_empty_to_none(row['Hit_Object_off_Carriageway'])
            JPD = convert_nan_and_empty_to_none(row['Journey_Purpose_of_Driver'])
            JL = convert_nan_and_empty_to_none(row['Junction_Location'])
            Mak = convert_nan_and_empty_to_none(row['make'])
            Mdl = convert_nan_and_empty_to_none(row['model'])
            SO = convert_nan_and_empty_to_none(row['Skidding_and_Overturning'])
            TO = convert_nan_and_empty_to_none(row['Towing_and_Articulation'])
            VLC = convert_nan_and_empty_to_none(row['Vehicle_Leaving_Carriageway'])
            VLRL = convert_nan_and_empty_to_none(row['Vehicle_Location.Restricted_Lane'])
            POI = convert_nan_and_empty_to_none(row['X1st_Point_of_Impact'])
            ABD = convert_nan_and_empty_to_none(row['Age_Band_of_Driver'])
            DID = convert_nan_and_empty_to_none(row['Driver_IMD_Decile'])
            EC = convert_nan_and_empty_to_none(row['Engine_Capacity_CC'])
            PC = convert_nan_and_empty_to_none(row['Propulsion_Code'])
            SD = convert_nan_and_empty_to_none(row['Sex_of_Driver'])
            VR = convert_nan_and_empty_to_none(row['Vehicle_Reference'])
            VM = convert_nan_and_empty_to_none(row['Vehicle_Manoeuvre'])
            VT = convert_nan_and_empty_to_none(row['Vehicle_Type'])
            WLHD = convert_nan_and_empty_to_none(row['Was_Vehicle_Left_Hand_Drive'])
            AG = convert_nan_and_empty_to_none(row['Age_of_Vehicle'])
            DHAT = convert_nan_and_empty_to_none(row['Driver_Home_Area_Type'])


            # Ensure that Accident_ID from Accident_Details matches Accident_ID from Accident_Time
            new_record = VehicleDetails(
                Accident_ID=vehicle_details_obj.Accident_ID,
                Accident_Index=row['Accident_Index'],
                Driver_Home_Area_Type=DHAT,
                Age_of_Vehicle=AG,
                Was_Vehicle_Left_Hand_Drive=WLHD,
                Vehicle_Type=VT,
                Vehicle_Manoeuvre=VM,
                Vehicle_Reference=VR,
                Sex_of_Driver=SD,
                Propulsion_Code=PC,
                Engine_Capacity_CC=EC,
                Driver_IMD_Decile=DID,
                Age_Band_of_Driver=ABD,
                X1st_Point_of_Impact=POI,
                Vehicle_Location_Restricted_Lane=VLRL,
                Vehicle_Leaving_Carriageway=VLC,
                Towing_and_Articulation=TO,
                Skidding_and_Overturning=SO,
                model=Mdl,
                make=Mak,
                Junction_Location=JL,
                Journey_Purpose_of_Driver=JPD,
                Hit_Object_off_Carriageway=HCOOUT,
                Hit_Object_in_Carriageway=HCOIN

            )

        session.add(new_record)
        counter += 1
        session.commit()

    # Commit the session to persist the changes to the database
   #session.commit()
    # Close the session
    session.close()
###
    with open('Vehicle_Details.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new records loaded into Vehicle Details = ' + str(counter)
        f.write(out_str)

def log_message(msg):
    with open('Vehicle_Details.txt','a') as f:
        f.write(f"{msg}\n")

csvpath='C:\\Users\\varsh\PycharmProjects\pythonProject1\Vehicles_extract.csv'

#####################################################################################
load_vehicle_details(csvpath, conn_string_db)
#####################################################################################
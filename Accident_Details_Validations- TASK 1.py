import pandas as pd
import os
import pyodbc
import urllib
import datetime
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Identity
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
class Accident(Base):
            __tablename__ = 'Accident_Details'
            Accident_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
            Accident_Index = Column(String(500))
            Accident_Severity = Column(String(255))
            Carriageway_Hazards = Column(String(255), nullable=True)
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
            Number_of_Casualties = Column(Integer)
            Number_of_Vehicles = Column(Integer)
            Pedestrian_Crossing_Human_Control = Column(Integer, nullable=True)
            Pedestrian_Crossing_Physical_Facilities = Column(Integer, nullable=True)
            Police_Force = Column(String(255))
            Special_Conditions_at_Site = Column(String(255), nullable=True)
            Speed_limit = Column(Integer, nullable=True)
            Urban_or_Rural_Area = Column(String(255))
            Weather_Conditions = Column(String(255))
            Year = Column(Integer)
            InScotland = Column(String(225), nullable=True)

# Create an ORM session connected to the dimensional database MyStoreDWETL using the connection-string conStrdw,
def convert_nan_and_empty_to_none(value):
    if pd.isna(value) or value == '':
        return None
    else:
        return value
def load_accident_details(csv_path,connection_string):
    engine= create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session =Session()
    counter = 0
    df=pd.read_csv(csv_path)
    df=df[['Accident_Index','Accident_Severity','Carriageway_Hazards','Did_Police_Officer_Attend_Scene_of_Accident','Junction_Control','Junction_Detail','Latitude',
           'Light_Conditions','Local_Authority_(District)','Local_Authority_(Highway)','Location_Easting_OSGR','Location_Northing_OSGR','Longitude',
           'LSOA_of_Accident_Location','Number_of_Casualties','Number_of_Vehicles','Pedestrian_Crossing-Human_Control','Pedestrian_Crossing-Physical_Facilities','Police_Force','Special_Conditions_at_Site',
           'Speed_limit','Urban_or_Rural_Area',	'Weather_Conditions','Year','InScotland']]

    for value, row in df.iterrows():

        if row['Accident_Severity'] not in ('Fatal', 'Serious', 'Slight'):
            log_message(f"Invalid Accident Severity Data in the csv file at {value}")
            continue
        if row['Urban_or_Rural_Area'] not in ('Rural', 'Unallocated','Urban'):
            log_message(f"Invalid Urban_or_Rural_Area Data in the csv file at {value}")
            continue
        if row['Year'] not in (2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016):
            log_message(f"Invalid Year Data in the csv file at {value}")
            continue


        police_officer= convert_nan_and_empty_to_none(row['Did_Police_Officer_Attend_Scene_of_Accident'])
        carr= convert_nan_and_empty_to_none(row['Carriageway_Hazards'])
        Acc = convert_nan_and_empty_to_none(row['Accident_Severity'])
        junction_control=convert_nan_and_empty_to_none(row['Junction_Control'])
        junction_detail = convert_nan_and_empty_to_none(row['Junction_Detail'])
        lat=convert_nan_and_empty_to_none(row['Latitude'])
        light=convert_nan_and_empty_to_none(row['Light_Conditions'])
        local_district=convert_nan_and_empty_to_none(row['Local_Authority_(District)'])
        local_high=convert_nan_and_empty_to_none(row['Local_Authority_(Highway)'])
        leo=convert_nan_and_empty_to_none(row['Location_Easting_OSGR'])
        lno=convert_nan_and_empty_to_none(row['Location_Northing_OSGR'])
        long=convert_nan_and_empty_to_none(row['Longitude'])
        lsoa=convert_nan_and_empty_to_none(row['LSOA_of_Accident_Location'])
        nc=convert_nan_and_empty_to_none(row['Number_of_Casualties'])
        nv=convert_nan_and_empty_to_none(row['Number_of_Vehicles'])
        pchc=convert_nan_and_empty_to_none(row['Pedestrian_Crossing-Human_Control'])
        pcpf=convert_nan_and_empty_to_none(row['Pedestrian_Crossing-Physical_Facilities'])
        pf=convert_nan_and_empty_to_none(row['Police_Force'])
        scas=convert_nan_and_empty_to_none(row['Special_Conditions_at_Site'])
        sl=convert_nan_and_empty_to_none(row['Speed_limit'])
        ura=convert_nan_and_empty_to_none(row['Urban_or_Rural_Area'])
        wc=convert_nan_and_empty_to_none(row['Weather_Conditions'])
        yr=convert_nan_and_empty_to_none(row['Year'])
        scl=convert_nan_and_empty_to_none(row['InScotland'])

        new_record = Accident(
                Accident_Index=row['Accident_Index'],
                Accident_Severity=Acc,
                Carriageway_Hazards=carr,
                Did_Police_Officer_Attend_Scene_of_Accident= police_officer,
                Junction_Control=junction_control,
                Junction_Detail=junction_detail,
                Latitude=lat,
                Light_Conditions=light,
                Local_Authority_District=local_district,
                Local_Authority_Highway=local_high,
                Location_Easting_OSGR=leo,
                Location_Northing_OSGR=lno,
                Longitude=long,
                LSOA_of_Accident_Location=lsoa,
                Number_of_Casualties=nc,
                Number_of_Vehicles=nv,
                Pedestrian_Crossing_Human_Control=pchc,
                Pedestrian_Crossing_Physical_Facilities=pcpf,
                Police_Force=pf,
                Special_Conditions_at_Site=scas,
                Speed_limit=sl,
                Urban_or_Rural_Area=ura,
                Weather_Conditions=wc,
                Year=yr,
                InScotland=scl
            )

        session.add(new_record)
        counter += 1
        session.commit()

    # Commit the session to persist the changes to the database
   #session.commit()
    # Close the session
    session.close()
###
    with open('Accident_Details_Log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new records loaded into Accidents Details = ' + str(counter)
        f.write(out_str)

def log_message(msg):
        with open('Accident_Details_Log.txt', 'a') as f:
            f.write(f"{msg}\n")

csvpath='C:\\Users\\varsh\PycharmProjects\pythonProject1\Accidents_extract_old.csv'

#####################################################################################
load_accident_details(csvpath, conn_string_db)
#####################################################################################
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


class AccidentRoadDetails(Base):
    __tablename__ = 'Accident_Road_Details'
    Road_ID = Column(Integer, primary_key=True)
    Accident_ID = Column(Integer, ForeignKey('Accident_Details.Accident_ID'))
    Accident_Index = Column(String(500))
    First_Road_Class = Column(String(255))  # Changed column name
    First_Road_Number = Column(Integer)  # Changed column name
    Second_Road_Class = Column(String(255))  # Changed column name
    Second_Road_Number = Column(Integer)  # Changed column name
    Road_Surface_Conditions = Column(String(255))
    Road_Type = Column(String(255))

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
    df = df[['Accident_Index','First_Road_Class', 'First_Road_Number', 'Second_Road_Class', 'Second_Road_Number','Road_Surface_Conditions', 'Road_Type']]


    for value, row in df.iterrows():

        if row['First_Road_Class'] not in ('A', 'A(M)', 'B', 'C', 'Motorways', 'Unclassified'):
            log_message(f"Invalid First_Road_Class data in the csv file at {value}")
            continue
        if row['Road_Surface_Conditions'] not in (
        'Data missing or out of range', 'Dry', 'Flood over 3cm. deep', 'Frost or ice', 'Snow', 'Wet or damp'):
            log_message(f"Invalid Road_Surface_Conditions data in the csv file at {value}")
            continue
        if row['Road_Type'] not in (
        'Dual carriageway', 'One way street', 'Roundabout', 'Single carriageway', 'Slip road', 'Unknown'):
            log_message(f"Invalid Road_Type data in the csv file at {value}")
            continue

        accident_details_obj = session.query(AccidentDetails).filter_by(
            Accident_Index=str(row['Accident_Index'])).first()

        if accident_details_obj:
            fc = convert_nan_and_empty_to_none(row['First_Road_Class'])
            frstn = convert_nan_and_empty_to_none(row['First_Road_Number'])
            sc = convert_nan_and_empty_to_none(row['Second_Road_Class'])
            sn = convert_nan_and_empty_to_none(row['Second_Road_Number'])
            Road = convert_nan_and_empty_to_none(row['Road_Surface_Conditions'])
            Type = convert_nan_and_empty_to_none(row['Road_Type'])
            # Ensure that Accident_ID from Accident_Details matches Accident_ID from Accident_Time
            new_record = AccidentRoadDetails(
                Accident_ID=accident_details_obj.Accident_ID,
                Accident_Index=row['Accident_Index'],
                First_Road_Class=fc,  # Corrected column name
                First_Road_Number=frstn,  # Corrected column name
                Second_Road_Class=sc,  # Corrected column name
                Second_Road_Number=sn,  # Corrected column name
                Road_Surface_Conditions=Road,
                Road_Type=Type
            )

        session.add(new_record)
        counter += 1
        session.commit()

    # Commit the session to persist the changes to the database
   #session.commit()
    # Close the session
    session.close()
###
    with open('Accident_Road_Details_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new records loaded into Accidents Road Details = ' + str(counter)
        f.write(out_str)

def log_message(msg):
    with open('Accident_Road_Details_log.txt','a') as f:
        f.write(f"{msg}\n")

csvpath='C:\\Users\\varsh\PycharmProjects\pythonProject1\Accidents_extract_old.csv'

#####################################################################################
load_accident_details(csvpath, conn_string_db)
#####################################################################################
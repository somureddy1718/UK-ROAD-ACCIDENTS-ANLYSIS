import pandas as pd
import os
import pyodbc
import urllib
import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Identity, Date, Time, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations

deprecations.SILENCE_UBER_WARNING = True

# Define the connection strings to the MyStore and MyStoreDWETL databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db

Base = declarative_base()

class AccidentDetails(Base):
    __tablename__ = 'Accident_Details'
    Accident_ID = Column(Integer, primary_key=True)
    Accident_Index = Column(String(500))

class AccidentTime(Base):
    __tablename__ = 'Accident_Time'
    Time_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
    Accident_ID = Column(Integer, ForeignKey('Accident_Details.Accident_ID'))
    Accident_Index = Column(String(500))
    Date = Column(Date)
    Day_of_Week = Column(String(255))
    Time = Column(Time)

def convert_nan_and_empty_to_none(value):
    if pd.isna(value) or value == '':
        return None
    else:
        return value

def load_accident_details(csv_path, connection_string):
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    counter = 0
    df = pd.read_csv(csv_path)
    df = df[['Accident_Index', 'Date', 'Day_of_Week', 'Time']]

    for value, row in df.iterrows():
        if row['Day_of_Week'] not in ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'):
            log_message(f"Invalid Day_of_Week data in the csv file at {value}")
            continue

        # Fetch AccidentDetails object by Accident_Index
        accident_details_obj = session.query(AccidentDetails).filter_by(Accident_Index=str(row['Accident_Index'])).first()

        if accident_details_obj:
            DW = convert_nan_and_empty_to_none(row['Day_of_Week'])
            Dt = convert_nan_and_empty_to_none(row['Date'])
            if Dt:
                Dt = datetime.datetime.strptime(row['Date'], '%d-%m-%Y').date()

            Tm = convert_nan_and_empty_to_none(row['Time'])
            if Tm:
                Tm = datetime.datetime.strptime(row['Time'], '%H:%M').time()

            New_record = AccidentTime(
                Accident_ID=accident_details_obj.Accident_ID,
                Accident_Index=row['Accident_Index'],
                Date=Dt,
                Day_of_Week=DW,
                Time=Tm,
            )

            session.add(New_record)
            counter += 1
            session.commit()
        else:
            print(f"No record found in Accident_Details for Accident_Index: {row['Accident_Index']}")

    session.close()

    with open('Accident_Time_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new records loaded into Accident Time = ' + str(
            counter)
        f.write(out_str)

def log_message(msg):
    try:
        with open('Accident_Time_log.txt', 'a') as f:
            f.write(f"{msg}\n")
    except Exception as e:
        print("Error creating log file:", e)

csvpath = 'C:\\Users\\varsh\\PycharmProjects\\pythonProject1\\Accidents_extract_old.csv'
load_accident_details(csvpath, conn_string_db)

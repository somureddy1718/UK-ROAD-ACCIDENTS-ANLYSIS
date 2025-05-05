import pandas as pd
import os
import pyodbc
import urllib
import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Date, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the connection strings to the MyStore and MyStoreDWETL databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_db)
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUKDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect={}".format(conn_string_dw)

Base = declarative_base()

class AccidentTimeDim(Base):
    __tablename__ = 'AccidentTimeDim'  # Changed from 'tablename' to '__tablename__'
    Date_ID = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(Date)
    Day_of_Week = Column(String(255))  # Specified a length for the string
    Time = Column(Time)

def convert_nan_and_empty_to_none(value):
    if pd.isna(value) or value == ' ':
        return None
    return value
def pipe_AccidentTime(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Accident_Time;'
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for value, row in dFrame.iterrows():
        if row['Day_of_Week'] not in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'):
            log_message(f"Invalid Day_of_Week data in the csv file at {value}")
            continue

        new_record = AccidentTimeDim(

            Date=convert_nan_and_empty_to_none(row['Date']),
            Day_of_Week=convert_nan_and_empty_to_none(row['Day_of_Week']),
            Time=convert_nan_and_empty_to_none(row['Time'])
        )
        session.add(new_record)
        counter += 1

    session.commit()  # Moved commit outside the loop to commit all changes at once
    session.close()

    # Logging
    log_activity(counter)

def log_activity(records_loaded):
    with open('AccidentTimeDim_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = "TimeStamp: {} --- Number of records loaded into AccidentTime Dimension: {}\n".format(current_time, records_loaded)
        log_file.write(log_message)

pipe_AccidentTime(conn_string_db, conn_string_dw)

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib.parse
import datetime

# Define the connection strings to the databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUK;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = f"mssql+pyodbc:///?odbc_connect={conn_string_db}"

conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=MANGO\\SQLEXPRESS;Database=AVUKDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = f"mssql+pyodbc:///?odbc_connect={conn_string_dw}"

Base = declarative_base()

class AccidentRoadDetailsDim(Base):
    __tablename__ = 'AccidentRoadDetailsDim'
    Road_ID = Column(Integer, primary_key=True)
    First_Road_Class = Column(String(255))
    First_Road_Number = Column(Integer)
    Second_Road_Class = Column(String(255), nullable=True)
    Second_Road_Number = Column(Integer, nullable=True)
    Road_Surface_Conditions = Column(String(255))
    Road_Type = Column(String(255))

def convert_nan_and_empty_to_none(value):
    if pd.isna(value) or value == '':
        return None
    return value

def pipe_road_details(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = 'SELECT * FROM Accident_Road_Details;'  # Modify query as needed to fit your data structure
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()

    try:
        for _, row in dFrame.iterrows():
            new_record = AccidentRoadDetailsDim(
                First_Road_Class=convert_nan_and_empty_to_none(row['First_Road_Class']),
                First_Road_Number=convert_nan_and_empty_to_none(row['First_Road_Number']),
                Second_Road_Class=convert_nan_and_empty_to_none(row['Second_Road_Class']),
                Second_Road_Number=convert_nan_and_empty_to_none(row['Second_Road_Number']),
                Road_Surface_Conditions=convert_nan_and_empty_to_none(row['Road_Surface_Conditions']),
                Road_Type=convert_nan_and_empty_to_none(row['Road_Type'])
            )
            session.add(new_record)
        session.commit()
    except Exception as e:
        session.rollback()
        log_activity(f"An error occurred: {str(e)}")
    finally:
        session.close()

    log_activity(len(dFrame))

def log_activity(message):
    with open('Accident_road_details_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into AccidentRoadDetailsDim: {message}\n"
        log_file.write(log_message)

# Execute the ETL process
pipe_road_details(conn_string_db, conn_string_dw)
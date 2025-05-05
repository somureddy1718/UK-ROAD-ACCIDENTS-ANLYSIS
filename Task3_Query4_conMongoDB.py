import pymongo
from tabulate import tabulate
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

def find_query(dbName, collectionName, query, projection, sort_col):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    query_output = collection.find(query, projection).sort(sort_col)
    documents = list(query_output)
    df = pd.DataFrame(documents)
    column_names = list(df.columns)  # Assuming 'find_one' might be unnecessary if list is not empty
    result_table = tabulate(df, headers=column_names, tablefmt="grid", showindex=False)
    return result_table

def aggregate_query(dbName, collectionName, pipeline):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    pipe_output = collection.aggregate(pipeline)
    documents = list(pipe_output)  # Convert cursor to list
    if documents:
        df = pd.DataFrame(documents)
        column_names = df.columns.tolist()  # Use list of column names from DataFrame
        result_table = tabulate(df, headers=column_names, tablefmt="grid", showindex=False)
        return result_table
    else:
        return "No documents found"

# Settings for vehicle database query
db_name = 'AccidentsVehicle'
collection_name = 'Vehicles'

# Define the aggregation pipeline for vehicle data by type and age band
pipeline = [{"$group": {"_id": {"Vehicle_Type": "$Vehicle_Type", "Age_Band_of_Driver": "$Age_Band_of_Driver", "Accident_Index": "$Accident_Index"}, "Number_of_Vehicles": {"$sum": 1}, "Average_Engine_Capacity": {"$avg": "$Engine_Capacity"}}},
    {"$sort": {"number_of_vehicles": -1}}, {"$project": {"_id": 0, "Accident_Index": "$_id.Accident_Index", "Vehicle_Type": "$_id.Vehicle_Type", "Age_Band_of_Driver": "$_id.Age_Band_of_Driver", "Number_of_Vehicles": 1, "Average_Engine_Capacity": 1}},{"$limit":5}]

print("Query", pipeline)

# Call the aggregation function and print the result
result_table = aggregate_query(db_name, collection_name, pipeline)
print("\nWhat is the distribution of vehicle types and age bands of drivers involved in accidents, along with the average engine capacity of these vehicles?\n",result_table)

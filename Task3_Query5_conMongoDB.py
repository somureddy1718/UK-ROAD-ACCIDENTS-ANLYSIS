import pymongo
from tabulate import tabulate
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

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

# Settings for the accident data aggregation
db_name = 'AccidentsVehicle'
collection_name = 'Accidents'

# Define the aggregation pipeline
pipeline = [{"$group":{"_id": {"Did_Police_Attend": "$Did_Police_Attend","Road_Class": "$Road_Class","Year": "$Year"},"Total_Accidents": {"$sum": 1},"Average_Number_of_Casualties": { "$avg": "$Number_of_Casualties" },}},{"$sort": { "Total_Accidents": -1 }},{"$project": { "_id": 0,"Did_Police_Attend": "$_id.Did_Police_Attend","Road_Class": "$_id.Road_Class","Year": "$_id.Year","Total_Accidents": 1,"Average_Number_of_Casualties": 1,"Accident_Indices": 1}}]

print("Query\n", pipeline)
# Call the aggregation function and print the result
result_table = aggregate_query(db_name, collection_name, pipeline)
print("\nHow do the total number of accidents, average number of casualties, and distribution of police attendance vary across different road classes and years?\n",result_table)

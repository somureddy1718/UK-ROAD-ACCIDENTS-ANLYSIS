import pymongo
from tabulate import tabulate
import pandas as pd
import json

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute a find query with sorting and projection in MongoDB
def find_query(dbName, collectionName, query, projection, sort_col):
    db = client[dbName]  # Accessing the database using a string variable
    collection = db[collectionName]  # Accessing the collection using a string variable
    query_output = collection.find(query, projection).sort(list(sort_col.items()))
    documents = list(query_output)
    if documents:  # Ensure there are documents returned
        df = pd.DataFrame(documents)
        column_names = list(df.columns)
        result_table = tabulate(df, headers=column_names, tablefmt="grid", showindex=False)
        return result_table
    else:
        return "No documents found."

# Define the database and collection names as variables
db_name = 'AccidentsVehicle'
collection_name = 'Accidents'

# Define the MongoDB query to find severe accidents in Scotland
query = {"Accident_Severity": "Serious","InScotland": "Yes"}
projection = {"Accident_Index": 1,"Accident_Severity": 1,"Date": 1,"Year": 1,"Local_Authority_(District)": 1,"Number_of_Casualties": 1, "_id" : 0}
sort_col = {"Date": -1}  # Sort by date in descending order

# Convert dictionaries to JSON strings
query_json = json.dumps(query, indent=2)
projection_json = json.dumps(projection, indent=2)
sort_col_json = json.dumps(sort_col, indent=2)

# Print the JSON strings
print("Query:")
print(query_json)

print("\nProjection:")
print(projection_json)

print("\nSort Column:")
print(sort_col_json)



# Call the query function and print the result
result_table = find_query(db_name, collection_name, query, projection, sort_col)
print("\nWhat are the details of the most recent severe accidents that occurred in Scotland, including the accident index, date, location, number of casualties, and time of the accident? \n", result_table)

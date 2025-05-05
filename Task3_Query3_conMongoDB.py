import pymongo
from tabulate import tabulate
import pandas as pd
import json

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

def find_query(dbName, collectionName, query, projection, sort_col, limit):
    db = client[dbName]  # Accessing the database using a string variable
    collection = db[collectionName]  # Accessing the collection using a string variable
    query_output = collection.find(query, projection).sort(list(sort_col.items())).limit(limit)
    documents = list(query_output)
    if documents:  # Ensure there are documents returned
        df = pd.DataFrame(documents)
        column_names = list(df.columns)
        result_table = tabulate(df, headers=column_names, tablefmt="grid", showindex=False)
        #print(result_table)
        return result_table
    else:
        return "No documents found."

# Define the database and collection names as variables
db_name = 'AccidentsVehicle'
collection_name = 'Vehicles'

# Define the MongoDB query to find severe accidents in Scotland
query = {"Sex_of_Driver": {"$in": ["Male"]}, "Journey_Purpose_of_Driver": "Commuting to/from work"}
projection = {"Year": 1, "Sex_of_Driver": 1, "Age_Band_of_Driver": 1, "Accident_Index": 1, "Date": 1, "Driver_Home_Area_Type": 1, "_id": 0}
sort_col = {"Date": -1}
limit = 10

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

print("\nLimit:")
print(limit)

# Call the query function and print the result
result_table = find_query(db_name, collection_name, query, projection, sort_col, limit)
print("\nWhat are the details of accidents involving drivers over the age of 75, including the year, date of the accident, accident index, drivers' sex, and their home area type? \n")
print(result_table)

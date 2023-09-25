#Import packages
import pymongo
import json
import credentials
import os

print("Current Working Directory:", os.getcwd())

connection_string = f"mongodb+srv://{credentials.username}:{credentials.password}@mongodb.4t0w0gd.mongodb.net/?retryWrites=true&w=majority"

client = pymongo.MongoClient(connection_string)

A3 = client['movies'] # Database movies is created

collection = A3['rating']  # 

# Load data from the JSON file
with open("movies_db.json", "r") as json_file:
    movie_data = json.load(json_file)

# Insert the data into the MongoDB collection
result = collection.insert_many(movie_data)


#Aggregation query 

#Average rating for each genre of movies

pipeline = [
    {
        "$group": {
            "_id": "$genre",
            "average_rating": {"$avg": "$rating"}
        }
    },
    {
        "$sort": {"average_rating": -1}
    }
]

# Execute the aggregation query
avg_rating_by_genre = list(collection.aggregate(pipeline))

#print the reults
for entry in avg_rating_by_genre:
    print(f"Genre: {entry['_id']}, Average Rating: {entry['average_rating']:.2f}")


with open("genre_average_rating.json", "w") as outfile:
    json.dump(avg_rating_by_genre, outfile, indent=4)
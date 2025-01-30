from pymongo import MongoClient
from urllib.parse import quote_plus
import os
from dotenv import load_dotenv

# Load environment variables from .env file (never expose secrets in code!)
load_dotenv(override=True)

# Fetch credentials securely
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")

# Build a URL-encoded connection string for special characters
connection_string = "mongodb+srv://%s:%s@%s" % (
    quote_plus(USER),  # Escape special symbols in username/password
    quote_plus(PASSWORD),
    HOST,
)

# Initialize MongoDB client and connect to the "sample_mflix" database
client = MongoClient(connection_string)
db = client["sample_mflix"]

# Example 1: Filter and Sort Movies
pipeline = [
    {
        "$match": {
            "year": {"$gte": 2010},  # Filter movies from 2010+
            "imdb.rating": {
                "$type": "number",
            },  # Filter movies with rating
        }
    },
    {"$sort": {"imdb.rating": -1}},  # Sort by IMDb rating (desc)
    {"$limit": 5},  # Get top 5
]

# Execute the pipeline
results = db.movies.aggregate(pipeline)

# Print formatted output
print("🎬 Top 5 Highest-Rated Movies (2010+):")
for movie in results:
    print(f"- {movie['title']} ({movie['year']}): IMDb {movie['imdb']['rating']}")

# Output: Top 5 highest-rated movies since 2010.
# 🎬 Top 5 Highest-Rated Movies (2010+):
# - A Brave Heart: The Lizzie Velasquez Story (2015): IMDb 9.4
# - The Real Miyagi (2015): IMDb 9.3
# - Human Planet (2011): IMDb 9.2
# - Over the Garden Wall (2014): IMDb 9.2
# - Frozen Planet (2011): IMDb 9.2

# Example 2: Group and Count Genres
pipeline = [
    {"$unwind": "$genres"},  # Split genres array into documents
    {
        "$group": {
            "_id": "$genres",  # Group by genre
            "total_movies": {"$sum": 1},  # Count movies per genre
        }
    },
    {"$sort": {"total_movies": -1}},  # Sort by most popular genres
    {"$limit": 5},  # Get top 5
]

# Execute the pipeline
results = db.movies.aggregate(pipeline)

# Print formatted output
print("\n🎭 Genre Popularity Ranking:")
for genre in results:
    print(f"- {genre['_id']}: {genre['total_movies']} movies")

# Output: Genre popularity ranking.
# 🎭 Genre Popularity Ranking:
# - Drama: 12385 movies
# - Comedy: 6532 movies
# - Romance: 3318 movies
# - Crime: 2457 movies
# - Thriller: 2454 movies

# Example 3: Join Comments with Movies using $lookup
pipeline = [
    {"$match": {"name": "Yara Greyjoy"}},  # Find comments by user "Zaki Ameer"
    {
        "$lookup": {
            "from": "movies",  # Join with movies
            "localField": "movie_id",  # Field in comments
            "foreignField": "_id",  # Field in movies
            "as": "movie_info",  # Save movie data here
        }
    },
    {
        "$project": {
            "user": "$name",
            "comment": "$text",
            "movie_title": {"$arrayElemAt": ["$movie_info.title", 0]},  # Extract title
        }
    },
    {"$limit": 3},  # Get only 3
]

# Execute the pipeline
results = db.comments.aggregate(pipeline)

# Print formatted output
print("\n💬 User Comments with Movie Titles:")
for comment in results:
    print(f"- User: {comment['user']}")
    print(f"  Movie: {comment['movie_title']}")
    print(f"  Comment: '{comment['comment'][:50]}...'")  # Truncate long comments

# Output: User Comments with Movie Titles.
# 💬 User Comments with Movie Titles:
# - User: Yara Greyjoy
#   Movie: Regeneration
#   Comment: 'Nobis incidunt ea tempore cupiditate sint. Itaque ...'
# - User: Yara Greyjoy
#   Movie: The Wizard of Oz
#   Comment: 'In velit quo asperiores aut debitis laborum fugiat...'
# - User: Yara Greyjoy
#   Movie: The Wizard of Oz
#   Comment: 'Corporis iste culpa reiciendis voluptatibus perspi...'

# Example 4: Calculate Average Ratings
pipeline = [
    {
        "$match": {
            "imdb.rating": {
                "$type": "number",
            },  # Filter movies with rating
        }
    },
    {
        "$group": {
            "_id": None,  # Group all documents
            "avg_rating": {"$avg": "$imdb.rating"},
            "max_rating": {"$max": "$imdb.rating"},
        }
    },
    {"$project": {"_id": 0}},  # Remove default `_id`
]

# Execute the pipeline
result = next(db.movies.aggregate(pipeline))

# Print formatted output
print(f"\n📊 IMDb Ratings Summary:")
print(f"- Average Rating: {result['avg_rating']:.1f}")
print(f"- Highest Rating: {result['max_rating']}")

# Output:
# 📊 IMDb Ratings Summary:
# - Average Rating: 6.7
# - Highest Rating: 9.6

# Example 5: Analyze Comments Over Time
pipeline = [
    {
        "$project": {
            "year": {"$year": "$date"},  # Extract year from date
            "text_length": {"$strLenCP": "$text"},  # Calculate comment length
        }
    },
    {"$group": {"_id": "$year", "avg_length": {"$avg": "$text_length"}}},
    {"$limit": 5},  # Get 5
]

# Execute the pipeline
results = db.comments.aggregate(pipeline)

# Print formatted output
print("\n📅 Average Comment Length by Year:")
for entry in sorted(results, key=lambda x: x["_id"]):
    print(f"- {entry['_id']}: {int(entry['avg_length'])} characters")

# Output: Average comment length per year.
# 📅 Average Comment Length by Year:
# - 1972: 152 characters
# - 1983: 152 characters
# - 1984: 151 characters
# - 2004: 150 characters
# - 2010: 154 characters

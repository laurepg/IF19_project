from pymongo import MongoClient
from bdd import ConnexionMongoDB

# Connect to the MongoDB database
client = ConnexionMongoDB()
db = client._db
collection = db["projet"]

# Initialize variables to calculate the averages
total_mentions_length = 0
total_hashtags_length = 0
mentions_count = 0
hashtags_count = 0

# Iterate through each document in the collection
for doc in collection.find():
    entities = doc.get('entities', {})
    
    # Process user mentions
    user_mentions = entities.get('user_mentions', [])
    for mention in user_mentions:
        screen_name = mention.get('screen_name', '')
        total_mentions_length += len(screen_name) + 1  # +1 for the "@" symbol
        mentions_count += 1
    
    # Process hashtags
    hashtags = entities.get('hashtags', [])
    for hashtag in hashtags:
        text = hashtag.get('text', '')
        total_hashtags_length += len(text) + 1  # +1 for the "#" symbol
        hashtags_count += 1

# Calculate the average lengths
if mentions_count > 0:
    avg_mentions_length = total_mentions_length / mentions_count
else:
    avg_mentions_length = 0

if hashtags_count > 0:
    avg_hashtags_length = total_hashtags_length / hashtags_count
else:
    avg_hashtags_length = 0

# Print the results
print(f'Average character cost of a mention (@): {avg_mentions_length}')
print(f'Average character cost of a hashtag (#): {avg_hashtags_length}')

import pandas as pd
from sklearnex import patch_sklearn
from bdd import ConnexionMongoDB
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

patch_sklearn()

# Replace with your MongoDB connection string
client = ConnexionMongoDB()
db = client._db

# Access the collection
collection = db.labelsWithUserData

# Define the query
query = {}
projection = {
    "_id": 0,  # Exclude the "_id" field
    "created_at": 1,
    "label": 1,
    "agressivity": 1,
    "visibility": 1,
    "ratio": 1,
    "ftweets": 1,
    "avgHashtag": 1,
    "avgMention": 1,
}

# Execute the query and fetch the data
cursor = collection.find(query, projection)
data = list(cursor)  # Convert cursor to list

# Convert to DataFrame
df = pd.DataFrame(data)

# Convert 'created_at' to datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# Convert datetime to Unix timestamp (number of seconds since 1970-01-01 00:00:00 UTC)
df['created_at'] = df['created_at'].apply(lambda x: x.timestamp())

# Print the DataFrame to check the data
print(df)

# Prepare data for supervised learning
X = df.drop('label', axis=1)  # Features
y = df['label']  # Target variable

# Print features and target variables to verify
print("Features (X):")
print(X)
print("\nTarget (y):")
print(y)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)

# Train a Random Forest classifier
clf = RandomForestClassifier(n_estimators=100, random_state=41)
clf.fit(X_train, y_train)

# Make predictions on the test set
y_pred = clf.predict(X_test)

# Evaluate the model's accuracy
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Print classification report for detailed evaluation
print("Classification Report:")
print(classification_report(y_test, y_pred))

# Compute the confusion matrix
matrix = confusion_matrix(y_test, y_pred)

plt.figure(figsize=(8, 6))
s = sns.heatmap(matrix, annot=True, 
            fmt='d', cmap='Blues', 
            annot_kws={"size": 16}, 
            xticklabels=['Non suspects', 'Suspects'], 
            yticklabels=['Non suspects', 'Suspects'])
plt.xlabel('Label prédit')
plt.ylabel('Label réel')
plt.title('Confusion Matrix')

# Save the plot to an image file
plt.savefig('confusionMatrix.png')

print("Confusion matrix saved as 'confusion.png'")

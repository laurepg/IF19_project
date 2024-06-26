from bdd import ConnexionMongoDB

conn = ConnexionMongoDB()
data = conn.userLabels()

print(data)


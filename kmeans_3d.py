import pandas as pd
import numpy as np

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn import cluster
from sklearn import metrics
from sklearn.preprocessing  import StandardScaler

import matplotlib.pyplot as plt

from bdd import ConnexionMongoDB

conn = ConnexionMongoDB()
data = conn.userTweets(10000)

# standardization
scaler = StandardScaler()
scaled_features = scaler.fit_transform(data)

# use K-means

#  Clusters = 4
kmeans = KMeans(n_clusters=4, random_state=42)
kmeans.fit(scaled_features)

labels = kmeans.labels_

acp = PCA()
coord = acp.fit_transform(scaled_features)

from mpl_toolkits.mplot3d import Axes3D

ax = plt.figure(1).add_subplot(projection='3d')
ax.scatter(coord[:,0], coord[:,1], coord[:,3], c=labels, cmap='viridis', s=50, alpha=0.5)
ax.view_init(60,35)
plt.suptitle("Représentation des individus selon les composantes 1, 2 et 4")
ax.set_xlabel('Comp. 1')
ax.set_ylabel('Comp. 2')
ax.set_zlabel('Comp. 4')
ax.set_xlim(-5, 20)
plt.show()

# Clusters = 2

kmeans2 = KMeans(n_clusters=2, random_state=42)
kmeans2.fit(scaled_features)

labels2 = kmeans2.labels_

ax = plt.figure(2).add_subplot(projection='3d')
ax.scatter(coord[:,0], coord[:,1], coord[:,3], c=labels2, cmap='viridis', s=50, alpha=0.5)
ax.view_init(60,35)
plt.suptitle("Représentation des individus selon les composantes 1, 2 et 4")
ax.set_xlabel('Comp. 1')
ax.set_ylabel('Comp. 2')
ax.set_zlabel('Comp. 4')
ax.set_xlim(-5, 20)
plt.show()
import csv
import numpy as np
import pandas as pd
from database import Database
from sklearn.preprocessing import MinMaxScaler
from minisom import MiniSom
from pylab import bone, pcolor, colorbar, plot, show

def preprocess(df, cols):
    # Split data
    X = df[['location_key', 'latitude', 'longitude']].values[:, [0,2,3]]
    X_train = df[cols]
    Y = df['fatal']
    
    x = None
    y = None
    z = None
    loc = False
    # Normalize coordinate data
    if 'latitude' in cols and 'longitude' in cols:
        f1 = lambda lat, lon: (np.cos(lat) * np.cos(lon)) 
        f2 = lambda lat, lon: (np.cos(lat) * np.sin(lon)) 
        f3 = lambda lat: np.sin(lat)
        x = f1(X_train['latitude'].values.astype(np.float), X_train['longitude'].values.astype(np.float))
        y = f2(X_train['latitude'].values.astype(np.float), X_train['longitude'].values.astype(np.float))
        z = f3(X_train['latitude'].values.astype(np.float))
        loc = True
    
    # Normalize rest of data
    if 'latitude' in cols: cols.remove('latitude')
    if 'longitude' in cols: cols.remove('longitude')
    X_train = pd.get_dummies(X_train[cols])
    sc = MinMaxScaler(feature_range = (0, 1))
    X_train = sc.fit_transform(X_train)
    
    # Combine data
    if loc:
        return [np.column_stack((x,y, z, X_train)), X]
    else:
        return [X_train, X]

def train_save(X_train, X, print_map=False, threshold=0.8, save_num=0):
    # Initialize SOM
    som = MiniSom(x = 10, y = 10, input_len = len(X_train[0]), sigma = 1.0, learning_rate = 0.5)
    som.random_weights_init(X_train)
    
    # Train
    print('Training')
    som.train_random(X_train, 1000)
    
    print('Making the map')
    d_map = som.distance_map().T
    
    # Find spots on map with high distance as outliers.
    # Get list of outliers
    print('Getting the outliers')
    d_clust = dict()
    d_dist = dict()
    for i, value in enumerate(X_train):
        # Get the winning node coordinates
        w = som.winner(value)
        if d_map[w[1]][w[0]] > threshold:
            # Add index to winning node dictionary
            if w in d_clust:
                d_clust[w].append(i)
            else:
                d_clust[w] = [i]
                d_dist[w] = d_map[w[1]][w[0]]
    
    # Get cluster data
    clusters = []
    num = 0
    for key, value in d_clust.items():
        #cluster = np.insert(X[value], 0, num, axis=1)[:].tolist()
        cluster = np.insert(np.insert(X[value].astype(np.float), 0, num, axis=1), 4, d_dist[key], axis=1).tolist()
        if clusters == []:
            clusters = cluster
        else:
            clusters.extend(cluster)
        num += 1
    print(str(num) + ' Clusters Found!')

    # Saving data
    print('Saving Results')
    with open('Clusters/schema-'+str(save_num)+'.ini', 'w', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerows([['[2014_clusters.csv]'], ['Col1=cluster INTEGER'], ['Col2=location_key INTEGER'], 
                          ['Col3=latitude DOUBLE'], ['Col4=longitude DOUBLE'], ['Col5=distance DOUBLE']])
    
    with open('Clusters/2014_clusters-'+str(save_num)+'.csv', 'w', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['cluster', 'location_key', 'latitude', 'longitude', 'distance'])
        writer.writerows(clusters) 

    # Create map
    if print_map:
        print('Printing Map')
        bone()
        pcolor(d_map)
        colorbar()
        
        for i, j in enumerate(X_train):
            # Get the winning node coordinates
            w = som.winner(j)
            # Place marker in centre of node
            plot(w[0] + 0.5, w[1] + 0.5, markerfacecolor = 'None',
                 markersize = 10, markeredgewidth = 2)
        show()
    print()

# Retrieve data from data mart
db = Database()
sql = ('SELECT * FROM fact_table F, location L, hour H, accident A '
       + ' WHERE F.location_key = L.location_key AND F.time_key = H.time_key AND F.accident_key = A.accident_key'
       + ' AND H.year = 2014')
data = db.query(sql, None, True, False)
df = pd.DataFrame.from_records(data[1:], columns=data[0])

# Map 1
X_train, X = preprocess(df, ['latitude', 'longitude', 'neighbourhood', 'fatal', 'intersection', 'num_pedestrians', 'holiday',
                             'environment', 'road_surface', 'traffic_control', 'visibility', 'impact_type'])
train_save(X_train, X, False, 0.8, 0)

# Map 2
X_train, X = preprocess(df, ['latitude', 'longitude', 'neighbourhood', 'holiday', 'weekend'])
train_save(X_train, X, False, 0.8, 1)

# Map 3
X_train, X = preprocess(df, ['latitude', 'longitude', 'holiday', 'weekend'])
train_save(X_train, X, False, 0.8, 2)

# Map 4
X_train, X = preprocess(df, ['latitude', 'longitude', 'visibility', 'time'])
train_save(X_train, X, False, 0.8, 3)

# Map 5
X_train, X = preprocess(df, ['latitude', 'longitude', 'time'])
train_save(X_train, X, False, 0.8, 4)

# Map 6
X_train, X = preprocess(df, ['time'])
train_save(X_train, X, True, 0, 5)
import importlib
import os.path
import site
import subprocess
import sys
from multiprocessing.dummy import Pool as ThreadPool
import pkg_resources

def install_package(package):
    try:
        pkg_resources.get_distribution(package)
    except pkg_resources.DistributionNotFound:
        query = [sys.executable, '-m', 'pip', 'install', package]
        if not hasattr(sys, 'real_prefix'):
            query = query + ['--user']
        subprocess.call(query)

# Verify that prerequisite packages are installed. Else install them and reload the list of packages available for
# importing
install_package('geopy')
install_package('holidays')
install_package('psycopg2-binary')
install_package('pandas')
install_package('psutil')
install_package('sklearn')
importlib.reload(site)
import collision
from database import Database
import weather
import pandas
import math

def load(collision_data, weather_data, location_data, hour_data):
    print('Loading Accident Dimension...')
    # Load accident dimension
    sql = 'INSERT INTO accident(accident_time, environment, road_surface, traffic_control, visibility, impact_type) VALUES(%s, %s, %s, %s, %s, %s) RETURNING accident_key;'
    accident_keys = db.insert_list(sql, collision_data[
        ['Time', 'Environment', 'Road_Surface', 'Traffic_Control', 'Light', 'Impact_type']].values.tolist())
    print('Loaded Accident Dimension!')
    
    # Load location dimension
    print('Loading Location Dimension...')
    location_keys = []
    if len(location_data) == 0:
        sql = 'INSERT INTO location(latitude, longitude, road_name, intersection1, intersection2) VALUES(%s, %s, %s, %s, %s) RETURNING location_key;' 
        location_keys = db.insert_list(sql, collision_data[['latitude', 'longitude', 'road_name', 'intersection1', 'intersection2']].values.tolist())
    else:
        sql = 'INSERT INTO location(latitude, longitude, road_name, intersection1, intersection2, neighbourhood) VALUES(%s, %s, %s, %s, %s, %s) RETURNING location_key;' 
        location_keys = db.insert_list(sql, location_data[['latitude', 'longitude', 'road_name', 'intersection1', 'intersection2', 'neighbourhood']].values.tolist())
    
    print('Loaded Location Dimension!')
    # Set spatial geom
    db.query('UPDATE location SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);', None)

    # Load weather dimension
    print('Loading Weather Dimension...')
    sql = 'INSERT INTO weather(station_name, latitude, longitude, temperature, type, visibility, wind_speed, wind_direction, pressure) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING weather_key;'
    db.insert_list(sql, weather_data[
        ['Station', 'Latitude', 'Longitude', 'Temperature', 'Weather', 'Visibility', 'Wind Speed', 'Wind Direction',
         'Pressure']].values.tolist())
    print('Loaded Weather Dimension!')

    # Load hour dimension
    print('Loading Hour Dimension...')
    sql = 'INSERT INTO hour(time, date, day_of_week, month, year, weekend, holiday, holiday_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) RETURNING time_key;'
    db.insert_list(sql, hour_data.values.tolist())
    print('Loaded Hour Dimension!')

    # Surrogate key pipeline, create fact table
    print('Preparing Fact Table...')
    fact = []
    for i in range(len(collision_data)):
        intersection = False
        fatal = False
        pedestrian = int(collision_data.iloc[i]['Pedestrian'])
        if '01' in collision_data.iloc[i]['Collision_Classification'].lower():
            fatal = True
        if ('intersection' in collision_data.iloc[i]['Collision_Location'].lower()) or (collision_data.iloc[i]['Traffic_Control'] != '10 - No control' and collision_data.iloc[i]['Traffic_Control'] != '00 - Unknown'):
            intersection = True
        sql = 'SELECT event_key FROM event WHERE start_date<=%(date)s and end_date>=%(date)s'
        events = db.query(sql, {'date': collision_data.iloc[i]['Date']})

        sql = 'SELECT time_key FROM hour WHERE date=%(date)s ORDER BY abs(extract(epoch from time - %(time)s)) LIMIT 2'
        time_keys = db.query(sql, {'date': collision_data.iloc[i]['Date'], 'time': collision_data.iloc[i]['Time']})
        sql = 'SELECT weather_key FROM weather WHERE weather_key = ANY (%(time_keys)s) ORDER BY (6371 * acos(cos(radians(%(latitude)s)) * cos(radians(latitude)) * cos(radians(longitude) - radians(%(longitude)s)) + sin(radians(%(latitude)s)) * sin(radians(latitude)))) limit 1'
        weather_key = db.query(sql, {'time_keys': time_keys,
                                     'latitude': collision_data.iloc[i]['Latitude'],
                                     'longitude': collision_data.iloc[i]['Longitude']})[0]
        time_key = weather_key
        fact.append([time_key, location_keys[i], accident_keys[i], weather_key, events, fatal, intersection,
                     pedestrian])
    
    # load fact table
    print('Loading Fact Table...')
    sql = 'INSERT INTO fact_table(time_key, location_key, accident_key, weather_key, event_key, fatal, intersection, num_pedestrians) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);'
    db.insert_list(sql, fact)
    print('Loaded Fact Table!')

def load_events(events):
    sql = 'INSERT INTO event(name, start_date, end_date) VALUES(%s, %s, %s);'
    db.insert_list(sql, events)

def historic_load():
    def transform(transform_function, data):
        print()
        pool = ThreadPool(64)
        results = pool.map(transform_function, data)
        pool.close()
        pool.join()
        # Combine the dataframes
        df = results[0]
        for result in results[1:]:
            df = df.append(result)
        return df

    location_data = []
    if os.path.isfile('data/collisions/ottawa/collision_data_transformed.csv'):
        print("Reading Transformed Collision Data...")
        collision_data = pandas.read_csv('./data/collisions/ottawa/collision_data_transformed.csv')
        print('Done Reading Transformed Collision Data!')
    else:
        # Extract data
        print('Extracting Collision Data...')
        collision_data = collision.extract_collision_data()
        # Transform the data with workers
        print('Transforming Collision Data...')
        print(len(collision_data))
        collision_data = transform(collision.transform_collision_data, collision_data)
        print('Transformed Collision Data!')
        # Save file for next time
        collision_data.to_csv('data/collisions/ottawa/collision_data_transformed.csv')
    if os.path.isfile('data/collisions/ottawa/location_dim_transformed.csv'):
        print('Reading Final Transformed Location Data...')
        location_data = pandas.read_csv('./data/collisions/ottawa/location_dim_transformed.csv')
        print('Done Reading Final Transformed Location Data!')
    elif os.path.isfile('data/collisions/ottawa/location_dim.csv'):
        print('Reading Transformed Location Data...')
        location_data = pandas.read_csv('./data/collisions/ottawa/location_dim.csv')
        print('Done Reading Transformed Location Data!')

    if os.path.isfile('data/weather/ontario/weather_data_transformed.csv'):
        print("Reading Transformed Weather Data...")
        weather_data = pandas.read_csv('data/weather/ontario/weather_data_transformed.csv', parse_dates=['Date_Time'],
                                       infer_datetime_format=True)
        print('Done Reading Transformed Weather Data!')
    else:
        print('Extracting Weather Data...')
        weather_data = weather.extract_weather_data(
            pandas.to_datetime(collision_data['Date'] + ' ' + collision_data['Time']))
        print('Transforming Weather Data...')
        weather_data = transform(weather.transform_weather_data, weather_data)
        print('Transformed Weather Data!')
        weather_data.to_csv('data/weather/ontario/weather_data_transformed.csv')

    if os.path.isfile('data/weather/ontario/hour_data_transformed.csv'):
        print("Reading Transformed Hour Data...")
        hour_data = pandas.read_csv('data/weather/ontario/hour_data_transformed.csv')  # FIXME dtypes
        print('Done Reading Transformed Hour Data!')
    else:
        print('Extracting Hour Data...')
        print('Transforming Hour Data...')
        hour_data = weather_data[['Date_Time']]
        import holidays
        holidays = holidays.Canada()
        hour_data = hour_data.merge(hour_data.Date_Time.apply(lambda datetime: pandas.Series(
            {'time': datetime.time(), 'date': datetime.date(), 'day_of_week': datetime.dayofweek,
             'month': datetime.month,
             'year': datetime.year, 'weekend': (False if datetime.dayofweek < 5 else True),
             'holiday': datetime.date() in holidays, 'holiday_name': holidays.get(datetime.date())})), left_index=True,
                                    right_index=True).drop(columns=['Date_Time'])
        print('Transformed Hour Data!')
        # hour_data.to_csv('data/weather/ontario/hour_data_transformed.csv')

    # Load data into database
    print('Loading Data...')
    print('Loading Events Dimension...')
    load_events(list(set(collision.events())))
    print('Loaded Events Dimension!')
    load(collision_data, weather_data, location_data, hour_data)

def refresh():
    db.delete_all()
    db.query(open("sql/spatial_schema.sql", "r").read(), None)
    historic_load()

# Historic Load
db = Database()
if db.is_connected():
    refresh()
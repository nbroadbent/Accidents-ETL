import pandas
from geopy import distance
from geopy.geocoders import ArcGIS

def extract_weather_data(collision_date_times):
    from psutil import virtual_memory

    chunksize = int(virtual_memory().total / 500)

    weather_data = []

    for file in [
        # './data/weather/ontario/weather_ontario_1_1.csv', './data/weather/ontario/weather_ontario_1_2.csv',
        './data/weather/ontario/weather_ontario_2_1.csv', './data/weather/ontario/weather_ontario_2_2.csv',
        # './data/weather/ontario/weather_ontario_3.csv', './data/weather/ontario/weather_ontario_4.csv'
    ]:
        for chunk in pandas.read_csv(file, parse_dates=['X.Date.Time'], infer_datetime_format=True, chunksize=chunksize,
                                     usecols=['X.Date.Time', 'Temp...C.', 'Wind.Dir..10s.deg.', 'Wind.Spd..km.h.',
                                              'Visibility..km.', 'Stn.Press..kPa.', 'Weather.',
                                              'X.U.FEFF..Station.Name.'],
                                     dtype={'Temp...C.': float, 'Stn.Press..kPa.': float, 'Weather.': str,
                                            'X.U.FEFF..Station.Name.': str}):
            chunk.rename(
                columns={'X.Date.Time': 'Date_Time', 'Temp...C.': 'Temperature', 'Wind.Dir..10s.deg.': 'Wind Direction',
                         'Wind.Spd..km.h.': 'Wind Speed', 'Visibility..km.': 'Visibility',
                         'Stn.Press..kPa.': 'Pressure', 'Weather.': 'Weather', 'X.U.FEFF..Station.Name.': 'Station'},
                inplace=True)
            chunk = chunk[chunk.Station.isin(weather_stations.Name)]
            if not chunk.empty:
                chunk = chunk[chunk.Date_Time >= '2013-01-01 00:00:00']
                if not chunk.empty:
                    chunk = chunk[chunk.Date_Time.apply(
                        lambda datetime: (
                                abs(collision_date_times.subtract(datetime).dt.total_seconds() / 60) < 30).any())]
                    if not chunk.empty:
                        chunk = pandas.merge(chunk, weather_stations[['Name', 'Latitude', 'Longitude']],
                                             left_on='Station', right_on='Name').drop(columns=['Name'])
                        weather_data.append(chunk)
    return weather_data


def transform_weather_data(df):
    df['Temperature'].interpolate(inplace=True)
    df['Wind Direction'].fillna(-1, inplace=True)
    df['Wind Speed'].fillna(-1, inplace=True)
    df['Visibility'].fillna(-1, inplace=True)
    df['Pressure'].fillna(-1, inplace=True)
    df['Weather'].fillna('Unknown', inplace=True)
    return df


def weather_stations():
    stations = pandas.read_csv('./data/weather/weather_stations.csv').drop(columns=['Latitude', 'Longitude'])
    stations = stations.rename(
        columns={'Latitude (Decimal Degrees)': 'Latitude', 'Longitude (Decimal Degrees)': 'Longitude'})
    stations = stations[['Name', 'Province', 'Latitude', 'Longitude', 'HLY Last Year']]
    ottawa = ArcGIS().geocode('Ottawa')
    radius = 15
    stations = stations[(stations.apply(
        lambda station: distance.distance((ottawa.latitude, ottawa.longitude),
                                          (station['Latitude'], station['Longitude'])).km < radius, axis=1)) & (
                                stations['HLY Last Year'] >= 2013)]
    return stations


weather_stations = weather_stations()
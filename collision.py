import pandas
import re
import xml.etree.ElementTree as ET

def analyze_data(df):
    # Check for null values, by column.
    null_cols = df.columns[df.isna().any()].tolist()
    print('Null Columns: ', null_cols)
    print('Null Amounts: ', df.isnull().sum())
    
    # View attribute data and look for errors
    for attribute in df.columns.tolist():
        # clean addresses
        if attribute in ['Record', 'Time', 'Date', 'Latitude', 'Longitude', 'X', 'Y']:
            continue
        print('\n'+attribute)
        print(set(df[attribute]))

def parse_date(date):
    return date[:4] + '-' + date[4:6] + '-' + date[6:8]

def find_nulls(df):
    pass

def events():
    root = ET.parse('data/events/Ottawa_Events.xml').getroot()
    data = []
    for event in root:
        start_date = parse_date(event[2].text).encode('utf-8').decode('latin-1')
        end_date = parse_date(event[3].text).encode('utf-8').decode('latin-1')
        name = event[4].text.encode('utf-8').decode('latin-1')
        data.append((name, start_date, end_date))
    return data

def make_collision_dim(df):
    return df[['Time', 'Environment', 'Road_Surface', 'Traffic_Control',
              'Light', 'Impact_type']]

def make_location_dim(df):
    return df[['Location', 'Latitude', 'Longitude']]

def extract_collision_data():
    data = []
    data.append(pandas.read_csv('./data/collisions/ottawa/2013collisions.csv'))
    data.append(pandas.read_csv('./data/collisions/ottawa/2014collisions.csv'))
    data.append(pandas.read_csv('./data/collisions/ottawa/2015collisions.csv'))
    data.append(pandas.read_csv('./data/collisions/ottawa/2016collisions.csv'))
    data.append(pandas.read_csv('./data/collisions/ottawa/2017collisions.csv'))
    return data
    
def transform_collision_data(df):
    # Remove unnecessary attributes.
    if hasattr(df, 'Year'):
        df = df.drop('Year', 1)
    if hasattr(df, 'Control_State'):
        df.drop('Control_State', 1)
    
    # Replace null values.
    df['Environment'].fillna('00 - Unknown', inplace=True)
    df['Traffic_Control'].fillna('00 - Unknown', inplace=True)
    df['Light'].fillna('00 - Unknown', inplace=True)
    df['Impact_type'].fillna('00 - Unknown', inplace=True)
    df['Pedestrian'].fillna(-1, inplace=True)
    df['Collision_Location'].fillna('00 - Unknown', inplace=True)
    
    print("Transforming Time")
    # Transform time to 24hour format
    i = 0
    for t in df['Time']:
        if len(t.split(':')) > 2:
            t = t.split(':')
            if 'AM' in t[2] or int(t[0]) == 12:
                t = t[0] + ':' + t[1]  
            else:
                t = str(int(t[0])+12) + ':' + t[1]
            df.iloc[i, df.columns.get_loc('Time')] = t
        elif len(t.split(':')) != 2:
            print('Error in time format!')
        i += 1
    
    print("Transforming dates")
    # Check for errors in dates, none.'
    i = 0
    for d in df['Date']:
        d = d.split('/')
        if (len(d) != 3):
            if '-' in d:
                print('Error in date format!')
            continue
        df.iloc[i, df.columns.get_loc('Date')] = d[2] + '-' + d[0] + '-' + d[1] 
        i += 1
    
    print("Transforming Streets")
    # Transform street info
    df['Street_Name'] = None
    df['Intersection1'] = None
    df['Intersection2'] = None
    i = 0
    for l in df['Location']:
        #l = 'Highway 417 btwn g st and h rd'
        l = l.replace('TO BE DETERMINED', '').replace('@', '').replace('&', '').replace('btwn', '').replace('/', ' ').lower()
        m = re.finditer(r'((.*?) (road|rd|ave|avenue|way|cres|st|street|dr|drive|pvt|private|pkwy|parkway|ramp))', l)
        # | (hwy|hiway|highway)(\w+))
        for j, s in enumerate(m):
            name = s.group().strip()
            #print('\n' +name)
            #if (name == None):
            #    print(i)
            if j == 0:
                df.iloc[i, df.columns.get_loc('Street_Name')] = name
            elif j == 1:
                df.iloc[i, df.columns.get_loc('Intersection1')] = name
            elif j == 2:
                df.iloc[i, df.columns.get_loc('Intersection2')] = name
            else:
                break
            if len(name) == 0:
                print(name)
                print(l)
        i += 1

    df['Street_Name'].fillna('00 - Unknown', inplace=True)
    df['Intersection1'].fillna('00 - Unknown', inplace=True)
    return df
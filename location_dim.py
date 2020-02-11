import pandas
loc = pandas.read_csv('./data/collisions/ottawa/location_dim.csv')

# Replace missing data from known data found in ArcGis
for i in range(len(loc)):
    # Replace unkown roads with roads from Ottawa data
    if loc.iloc[i]['road_name'] == '00 - Unknown' or loc.iloc[i]['road_name'] == 'st':
        loc.iloc[i, loc.columns.get_loc('road_name')] = loc.iloc[i]['ROAD_NAMES'].lower()
    if loc.iloc[i]['road_name'] == loc.iloc[i]['intersection1']:
        # Transfer intersection 2 to intersection 1 if exists and road is same as intersection 1
        if loc.iloc[i]['intersection2'] != '00 - Unknown' and loc.iloc[i]['intersection2'] != '<Null>' and loc.iloc[i]['intersection2'] != None:
            loc.iloc[i, loc.columns.get_loc('intersection1')] = loc.iloc[i]['intersection2']
            loc.iloc[i, loc.columns.get_loc('intersection2')] = None
    elif loc.iloc[i]['intersection1'] == '00 - Unknown' or loc.iloc[i]['intersection1'] == 'st':
        # road and intersection aren't the same and intersection is null
        loc.iloc[i, loc.columns.get_loc('intersection1')] = loc.iloc[i]['ROAD_NAMES'].lower()
# Column no longer needed
loc = loc.drop('ROAD_NAMES', 1)
# Observe new data
null_cols = loc.columns[loc.isna().any()].tolist()
print('Null Columns: ', null_cols)
print('Null Amounts: ', loc.isnull().sum())
# Save Data
loc.to_csv('data/collisions/ottawa/location_dim_transformed.csv')
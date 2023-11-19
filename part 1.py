import pandas as pd
from math import sqrt
from multiprocessing import Pool


def dms_to_dd(dms):
    degrees = int(dms) // 100
    minutes = (int(dms) % 100) / 60
    return degrees + minutes


def geodistance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + math.cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * asin(sqrt(a)) * 6371


def generate_station_dict(county, unique_stations):
    stations_dict = {}
    for idx1, row1 in county.iterrows():
        lon, lat = row1['LON'], row1['LAT']  # each county/city
        stations_within_200km = []
        for idx2, row2 in unique_stations.iterrows():
            Di = geodistance(lon, lat, row2['longitude'], row2['latitude'])  # Distance calculation
            if Di == 0 or (0 < Di < 200):
                stations_within_200km.append((row2['station'], row2['longitude'], row2['latitude'], Di))
        stations_dict[row1['NAME']] = stations_within_200km
    return stations_dict


def main_1():
    county = pd.read_excel('./county_geo_new.xlsx')  # Contains coordinates of urban districts
    data_dir = 'D:\projects\dta_new'

    unique_stations = pd.DataFrame(columns=['station', 'longitude', 'latitude'])

    for f in os.listdir(data_dir):
        weather_df = pd.read_stata(os.path.join(data_dir, f))
        weather_df['longitude'], weather_df['latitude'] = weather_df['longitude'].apply(dms_to_dd), weather_df[
            'latitude'].apply(dms_to_dd)
        unique_stations = pd.concat(
            [unique_stations, weather_df[['station', 'longitude', 'latitude']]]).drop_duplicates()

    # Generate dict
    data_dict = generate_station_dict(county, unique_stations)

    # Store results as json file (json format maintains dictionary structure)
    import json
    with open('stations_dict.json', 'w') as file:
        json.dump(data_dict, file)


if __name__ == '__main__':
    main_1()

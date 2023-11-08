import pandas as pd
import os
import math
from math import sin, cos, asin, sqrt


def geodistance(lng1, lat1, lng2, lat2):
    # convert latitudes and longitudes (decimal degrees) to radians
    lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    # Haversine formula to calculate the square of half the chord length between two points
    a = sin(dlat / 2) ** 2 + math.cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    # Distance in kilometer calculated by angular distance in radians and the Earth radius (6371km)
    return 2 * asin(sqrt(a)) * 6371


def interpolate_dynamic(lon, lat, period, weather_vars):
    sums = dict.fromkeys(weather_vars, 0)
    # return a new dictionary with the specified keys (must unique), all with 0 as value (only one but can be a list)
    w_sums = dict.fromkeys(weather_vars, 0)  # Dictionary to store the sum for each column

    # power of the IDW setting
    P = 2
    for idx2, row2 in period.iterrows():  # loop over station obs records for one day
        Di = geodistance(lon, lat, row2['LON'],
                         row2['LAT'])  # Calculate distance between county center & station locations

        if 0 < Di < 200:  # Include data from stations in a search radius of 200km
            wi = 1 / (Di ** P)  # Compute weight
            # loop over two lists together, simultaneously retrieves one element from both lists in each iteration
            for column in weather_vars:
                # call up the function to judge whether the data should be included in calculation
                var = (row2[column])
                sums[column] += var * wi
                w_sums[column] += wi
    # store the daily interpolated weather values in a dictionary format
    return {column: round(sums[column] / w_sums[column], 4) if w_sums[column] != 0 else 32766 for column in
            weather_vars}


if __name__ == '__main__':
    county = pd.read_excel('./county_geo_new.xlsx')
    dirs = os.listdir('./test\splitday')
    results = []
    for file_path in dirs:
        df = pd.read_csv('./test\splitday/' + file_path)
        for idx, row in county.iterrows():
            lon = row['LON']
            lat = row['LAT']
            result = interpolate_dynamic(lon=lon, lat=lat, period=df,
                                         weather_vars=['avg_temp', 'high_temp', 'low_temp']
                                         )
            results.append([row['NAME'], row['PAC'], *result.values()])
        out = pd.DataFrame(results)
        out.to_csv('./test/newint/' + file_path,
                   header=['countyname', 'countycode', 'avg_temp', 'high_temp', 'low_temp'])
        print('finish \t' + file_path)

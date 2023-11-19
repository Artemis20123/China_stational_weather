import pandas as pd
import json
from multiprocessing import Pool
import os
from math import radians, sin, cos, asin
import math


# Keep your previous functions and update functions for necessary modifications
# Note that generate_station_dict function is not needed for step 2, so remove it from step 2.

def interpolate_dynamic(lon_lat, period, weather_vars, quality_code):
    lon, lat = lon_lat
    sums = dict.fromkeys(weather_vars, 0)
    w_sums = dict.fromkeys(weather_vars, 0)
    P = 2
    for idx2, row2 in period.iterrows():
        for station_info in data_dict[str(lon_lat)]:
            station_id, lng, lat, Di = station_info
            if lng == row2['longitude'] and lat == row2['latitude']:
                wi = 1 / (Di ** P)
                for column, quality_column in zip(weather_vars, quality_code):
                    var = quality_control(row2[column], row2[quality_column], column)
                    if var is not None:
                        re_var = rescale(var, column)
                        sums[column] += re_var * wi
                        w_sums[column] += wi
    return {column: round(sums[column] / w_sums[column], 4) if w_sums[column] != 0 else 32766 for column in
            weather_vars}


# Continue for remaining functions as described earlier

def main_2():
    county = pd.read_excel('./county_geo_new.xlsx')
    data_dir = 'D:\projects\dta_new'
    output_dir = 'D:\projects\output'

    weather_vars = ['TEM_var8', 'TEM_var9', 'TEM_var10', 'RHU_var8', 'RHU_var9', 'PRE_var8', 'PRE_var9', 'PRE_var10']
    quality_code = ['TEM_var11', 'TEM_var12', 'TEM_var13', 'RHU_var10', 'RHU_var11', 'PRE_var11', 'PRE_var12',
                    'PRE_var13']

    with open('stations_dict.json', 'r') as file:
        data_dict = json.load(file)

    for f in os.listdir(data_dir):
        weather_df = pd.read_stata(os.path.join(data_dir, f))
        weather_df['longitude'], weather_df['latitude'] = weather_df['longitude'].apply(dms_to_dd), weather_df[
            'latitude'].apply(dms_to_dd)
        weather_df['datetime'] = pd.to_datetime(weather_df[['year', 'month', 'day']]).dt.date
        weather_df[weather_vars] = weather_df[weather_vars].fillna(32766)

        with Pool() as pool:
            pool.map(speedup_interpolation,
                     [(weather_df, day, county, weather_vars, quality_code, output_dir, data_dict) for day in
                      weather_df['datetime'].unique()])


if __name__ == '__main__':
    main_2()

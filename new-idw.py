import pandas as pd
import os
import math
from math import sin, cos, asin, sqrt
from itertools import product

# power of the IDW setting
P = 2


# Function to convert degrees minute to decimal degrees
def dms_to_dd(dms):
    degrees = int(dms) // 100
    minutes = (int(dms) % 100) / 60
    return degrees + minutes


# Function to compute geodistance between two points on earth
def geodistance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])  # 经纬度(度十进制分)转换成弧度
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + math.cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * asin(sqrt(a)) * 6371  # 地球平均半径，6371km


# Function to handle abnormal records
def quality_control(var, code, column):
    if code != 0:  # Exclude non-zero control codes
        return None
    if var == 32700:  # For special code 32700 in pre columns, replace them with specific values
        if column in {'pre_var8', 'pre_var9'}:
            return 0.5
        elif column == 'pre_var10':
            return 1
    if var > 30000:  # Exclude abnormal values
        return None
    return var


# Dynamic IDW interpolation of weather data at a specific lon, lat
def interpolate_dynamic(lon, lat, stations):
    columns = ['tem_var8', 'tem_var9', 'tem_var10', 'rhu_var8', 'rhu_var9', 'pre_var8', 'pre_var9', 'pre_var10']
    quality_code_columns = ['tem_var11', 'tem_var12', 'tem_var13', 'rhu_var10', 'rhu_var11', 'pre_var11', 'pre_var12',
                            'pre_var13']
    sums = dict.fromkeys(columns, 0)
    w_sums = dict.fromkeys(columns, 0)  # Dictionary to store the sum for each column
    for index, point in stations.iterrows():
        Di = geodistance(lon, lat, point['LON'], point['LAT'])  # Calculate distance
        if 0 < Di < 200:  # Include data from stations in a radius of 200
            for column, quality_code_column in zip(columns, quality_code_columns):
                var = quality_control(point[column], point[quality_code_column], column)
                if var is not None:  # If the variable value is valid, use it for interpolation
                    wi = 1 / (Di ** P)  # Compute weight
                    sums[column] += var * wi  # Cumulative sum of the product of the variable and weight
                    w_sums[column] += wi  # Cumulative sum of the weights
    # Compute the interpolated value for each variables
    return {column: round(sums[column] / w_sums[column], 4) if w_sums[column] != 0 else 32766 for column in
            columns}  # represent missing data as 32766

# Overall function (gathering results)
def apply_interpolation(data, day, county, columns):
    # select records in the weather data for a specific day
    stations = data.loc[data['datetime'] == day]
    results = []
    # for loop for each county with center lons & lats
    for idx1, district in county.iterrows():
        lon, lat = district['LON'], district['LAT'] # each city
        result = interpolate_dynamic(lon=lon, lat=lat, stations=stations)
        results.append([district['NAME'], district['PAC'], day, *result.values()]) #a value array in the result series; * before used to unpack the values
    return results


def main():
    county = pd.read_excel('D:\projects\weather\county_test.xlsx')  # Contains coordinates of urban districts

    # getting formatted values from the dta files
    column_vars = {  # Variables to be interpolated, manually updated
        'tem': ['var8', 'var9', 'var10'],
        'rhu': ['var8', 'var9'],
        'pre': ['var8', 'var9', 'var10'],
    }

    column_codes = {  # Corresponding control codes
        'tem': ['var11', 'var12', 'var13'],
        'rhu': ['var10', 'var11'],
        'pre': ['var11', 'var12', 'var13']
    }
    data_dir = 'D:\projects\weather/test'
    vars = [f"{kind}_{suffix}" for kind in column_vars.keys() for suffix in column_vars[kind]]
    codes = [f"{kind}_{suffix}" for kind in column_codes.keys() for suffix in column_codes[kind]]

    for f in os.listdir(data_dir):
        data = pd.read_stata(os.path.join(data_dir, f))
        data['LON'], data['LAT'] = data['LON'].apply(dms_to_dd), data['LAT'].apply(dms_to_dd)
        data['datetime'] = pd.to_datetime(data[['year', 'month', 'day']])
        # if NA exists in the value columns, filled with 32766
        data[vars + codes] = data[vars + codes].fillna(32766)

        for day in data['datetime'].unique():
            results = apply_interpolation(data, day, county, vars)
            df = pd.DataFrame(results, columns=['countyname', 'countycode', 'day', *vars])
#            df1 = df.drop_duplicates()
            df.to_csv(os.path.join('./', f'{day.year}.csv'), index=False,
                      mode='a' if os.path.exists(f'{day.year}.csv') else 'w')


if __name__ == '__main__':
    main()

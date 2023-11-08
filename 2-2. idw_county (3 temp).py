import math
import os
from math import sin, cos, asin, sqrt
import pandas as pd

# power of the IDW setting
P = 2

def geodistance(lng1, lat1, lng2, lat2):
    lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])  # 经纬度(度十进制分)转换成弧度
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + math.cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * asin(sqrt(a)) * 6371  # 地球平均半径，6371km

# lon和lat分别是要插值的点的x,y
# lst是已有数据的数组，结构为：[[x1，y1，z1]，[x2，y2，z2]，...]
# 返回值是插值点的三种温度
def interpolation(lon, lat, lst):
    # Function to interpolate the temperature values using IDW method for all three temperature fields: avg_temp, high_temp, and low_temp
    sum0_avg = 0
    sum0_high = 0
    sum0_low = 0
    sum1_avg = 0
    sum1_high = 0
    sum1_low = 0
    for point in lst:
        if lon == point[0] and lat == point[1]:    # Return the actual temperature values if the point matches exactly
            # Check if the temperature values are missing (3276.6) and replace with -9999999 if true
            avg_temp = point[2] if point[2] < 3276 else -9999999
            high_temp = point[3] if point[3] < 3276 else -9999999
            low_temp = point[4] if point[4] < 3276 else -9999999
            return avg_temp, high_temp, low_temp

        Di = geodistance(lon, lat, point[0], point[1])
        if 0 < Di < 200:
            # Check if the point has usable data for avg_temp before including it in the avg_temp interpolation
            if point[2] < 3276:
                wi_avg = 1 / (Di ** P)
                sum0_avg += point[2] * wi_avg
                sum1_avg += wi_avg

            if point[3] < 3276:
                wi_high = 1 / (Di ** P)
                sum0_high += point[3] * wi_high
                sum1_high += wi_high

            if point[4] < 3276:
                wi_low = 1 / (Di ** P)
                sum0_low += point[4] * wi_low
                sum1_low += wi_low

    # Interpolated values for avg_temp, high_temp, and low_temp
    avg_temp_interp = round(sum0_avg / sum1_avg, 4) if sum1_avg != 0 else -9999999
    high_temp_interp = round(sum0_high / sum1_high, 4) if sum1_high != 0 else -9999999
    low_temp_interp = round(sum0_low / sum1_low, 4) if sum1_low != 0 else -9999999
    return avg_temp_interp, high_temp_interp, low_temp_interp

all_district = []

def read_data():
    county = pd.read_excel('./county_geo_new.xlsx')

    for idx, row in county.iterrows():
        all_district.append(
            {'name': row['NAME'], 'countycode': row['PAC'], 'lon': row['LON'], 'lat': row['LAT']})


if __name__ == '__main__':
    read_data()
    dirs = os.listdir('./test\splitday')
    exist_dirs = set(os.listdir('./test/county_int'))
    for file_path in dirs:
        if exist_dirs.__contains__(file_path):
            continue
        df = pd.read_csv('./test\splitday/' + file_path)
        stations = []
        for idx, row in df.iterrows():
            x, y, avg_temp, high_temp, low_temp = row['LON'], row['LAT'], row['avg_temp'], row['high_temp'], row['low_temp']
            stations.append([x, y, avg_temp, high_temp, low_temp])
        results = []
        for district in all_district:
            lon = district['lon']
            lat = district['lat']
            avg_temp_interp, high_temp_interp, low_temp_interp = interpolation(lon=lon, lat=lat, lst=stations)
            results.append([str(district['name']), str(district['countycode']), avg_temp_interp, high_temp_interp, low_temp_interp])

        out = pd.DataFrame(results)
        out.to_csv('./test/county_int/' + file_path, header=['countyname', 'countycode', 'avg_temp', 'high_temp', 'low_temp'])
        print('finish \t' + file_path)
import pandas as pd
import os
import math
from math import sin, cos, asin, sqrt


# convert lons & lats unit of degrees minute to decimal degrees
def dms_to_dd(dms):
    degrees = int(dms) // 100
    minutes = (int(dms) % 100) / 60
    return degrees + minutes


# compute geographic distance between two points on earth
def geodistance(lng1, lat1, lng2, lat2):
    # convert latitudes and longitudes (decimal degrees) to radians
    lng1, lat1, lng2, lat2 = map(math.radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    # Haversine formula to calculate the square of half the chord length between two points
    a = sin(dlat / 2) ** 2 + math.cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    # Distance in kilometer calculated by angular distance in radians and the Earth radius (6371km)
    return 2 * asin(sqrt(a)) * 6371


# handle abnormal records via quality control code identification
def quality_control(var, code, column):
    if code != 0:  # Exclude non-zero control codes
        return None

    if column == 'rhu_var9' and var > 300:  # exclusion for relative humidity
        return None
    if var == 32700:  # For special code 32700 (微量降水) in precipitation columns, replace them with specific values
        if column in {'pre_var8', 'pre_var9'}:  # half-day measurement
            return 0.5
        elif column == 'pre_var10':  # a single day measurement
            return 1  # according to the definition of 微量降水: < 0.1mm a day

    if var > 30000:  # Exclude other potential characteristic values (e.g.,30XXX: snow amount)
        return None
    return var


# Dynamic IDW interpolation of weather data at a specific lon, lat
def interpolate_dynamic(lon, lat, period, weather_vars, quality_code):
    sums = dict.fromkeys(weather_vars, 0)
    # return a new dictionary with the specified keys (must unique), all with 0 as value (only one but can be a list)
    w_sums = dict.fromkeys(weather_vars, 0)  # Dictionary to store the sum for each column

    # power of the IDW setting
    P = 2
    for idx2, row2 in period.iterrows():  # loop over station obs records for one day
        Di = geodistance(lon, lat, row2['LON'],
                         row2['LAT'])  # Calculate distance between county center & station locations
        if Di == 0:
            return {exact: quality_control(row2[exact], row2[quality_code[idx3]], exact)
                    for idx3, exact in enumerate(weather_vars)}

        if 0 < Di < 200:  # Include data from stations in a search radius of 200km
            wi = 1 / (Di ** P)  # Compute weight
            # loop over two lists together, simultaneously retrieves one element from both lists in each iteration
            for column, quality_column in zip(weather_vars, quality_code):
                # call up the function to judge whether the data should be included in calculation
                var = quality_control(row2[column], row2[quality_column], column)
                if var is not None:  # If the variable value is valid, use it for interpolation
                    sums[column] += var * wi
                    w_sums[column] += wi
    # store the daily interpolated weather values in a dictionary format
    return {column: round(sums[column] / w_sums[column], 4) if w_sums[column] != 0 else 32766 for column in
            weather_vars}


# Overall function (generate daily results)
def apply_interpolation(data, date, level, weather_vars, quality_code):
    # select records in the weather data for a specific day
    daily = data.loc[data['datetime'] == date]
    results = []
    # for loop for each county with center lons & lats
    for idx1, row1 in level.iterrows():
        lon, lat = row1['LON'], row1['LAT']  # each county/city
        result = interpolate_dynamic(lon=lon, lat=lat, period=daily, weather_vars=weather_vars,
                                     quality_code=quality_code)
        results.append([row1['NAME'], row1['PAC'], date, *result.values()])
        # a value array in the result series; * before used to unpack the values
    return results


def main():
    county = pd.read_excel('D:\projects\weather\county_test.xlsx')  # Contains coordinates of urban districts
    data_dir = 'D:\projects\weather/test'
    output_dir = 'D:\projects\weather\out'

    weather_vars = ['tem_var8', 'tem_var9', 'tem_var10', 'rhu_var8', 'rhu_var9', 'pre_var8', 'pre_var9', 'pre_var10']
    quality_code = ['tem_var11', 'tem_var12', 'tem_var13', 'rhu_var10', 'rhu_var11', 'pre_var11', 'pre_var12',
                    'pre_var13']

    for f in os.listdir(data_dir):
        # Extracts the year from the input filename
        infor = f.split('-')[-1].split('.')[0]
        # Constructs the expected output filename
        output_file_path = os.path.join(output_dir, f"{infor}.csv")
        # Check if output_file_path already exists
        if os.path.exists(output_file_path):
            continue

        weather_df = pd.read_stata(os.path.join(data_dir, f))
        weather_df['LON'], weather_df['LAT'] = weather_df['LON'].apply(dms_to_dd), weather_df['LAT'].apply(dms_to_dd)
        # convert to timestamp and exclude 'hour:min:sec' information
        weather_df['datetime'] = pd.to_datetime(weather_df[['year', 'month', 'day']]).dt.date
        # if NA exists in the value columns, filled with 32766
        weather_df[weather_vars] = weather_df[weather_vars].fillna(32766)

        outputs = []
        # merge interpolation results for multiple days
        for day in weather_df['datetime'].unique():
            print(day)
            output = apply_interpolation(weather_df, day, county, weather_vars, quality_code)
            outputs.extend(output)  # use 'extend' instead of 'append' to keep 'output' data structure

        df = pd.DataFrame(outputs,
                          columns=['countyname', 'countycode', 'date', 'avg_tem', 'max_tem', 'min_tem', 'avg_rhu',
                                   'min_rhu', 'eve_pre', 'mor_pre', 'day_pre'])
        df.to_csv(os.path.join(output_dir, f'{day.year}.csv'), index=False, encoding='utf_8_sig')
        # f-string formatting to include the year of the 'date' variable


if __name__ == '__main__':
    main()

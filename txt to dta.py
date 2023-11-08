import pandas as pd
import os
from glob import glob

root_path = 'D:\projects\SURF_CLI_CHN_MUL_DAY_V3.0\datasets'

weather_types = [name for name in os.listdir(root_path)]
column_names = ['station_id', 'latitude', 'longitude', 'height', 'year', 'month', 'day']

# Initialize nested dict for storing filepathes of year (outer key) and weather (inner key)
filepaths_dict = {year: {weather: [] for weather in weather_types} for year in range(1951, 2016)}

# Gather all filepaths for each year and weather type
for weather in weather_types:
    filepaths = glob(os.path.join(root_path, weather, "*.TXT"))
    for filepath in filepaths:
        year = int(filepath.split('-')[-1][:4])
        filepaths_dict[year][weather].append(filepath)

# Processing each year
for year in range(1951, 2016):
    df_year = None
    # Processing each weather type
    for weather in weather_types:
        print(year, weather)
        df_weather = pd.concat(
            (pd.read_table(file, sep=r"\s+", header=None) for file in filepaths_dict[year][weather]))
        duplicates = df_weather.duplicated(subset=[0, 1, 2, 4, 5, 6])
        if any(duplicates):
            print(f"Found duplicates in year {year}, weather {weather}. Operation stopped.")
            exit()
        df_weather.columns = column_names + [f'{weather}_var{i}' for i in range(8, df_weather.shape[1] + 1)]

        # Prepend df_weather to df_year
        if df_year is None:
            df_year = df_weather  # store the annual data for a single weather type
        else:
            df_year = pd.merge(df_year, df_weather, on=column_names, how='outer')

    df_year.to_stata(f"D:\\projects\\dta_new\\SURF_CLI_CHN_MUL_DAY-{year}.dta", write_index=False)
    print(f"Saved data for year {year} into .dta file.")

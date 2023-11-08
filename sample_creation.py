import pandas as pd

a = pd.read_stata ('D:\projects\dta_new\SURF_CLI_CHN_MUL_DAY-1951.dta')
# Create a DataFrame
data = pd.DataFrame({
    'year': [1995, 1995, 1995, 1995, 1995, 1995],
    'month': [1, 1, 1, 1, 1, 1],
    'day': [1, 1, 1, 2, 2, 2],
    'LON': [11558, 11652, 11625, 11558, 11652, 11625],  # Longitude represented in degrees and minutes
    'LAT': [4027, 4023, 4025, 4027, 4023, 4025],  # Latitude represented in degrees and minutes
    'tem_var8': [6.0, 5.0, 32766, 32766, 4.0, 5.0],  # Average temperature
    'tem_var9': [32766, 7.0, 13.0, 8.0, 32766, 12.0],  # Maximum temperature
    'tem_var10': [2.0, 32766, 32766, 1.0, 0.0, 32766],  # Minimum temperature
    'rhu_var8': [85, 80, 90, 75, 80, 85],  # Average relative humidity
    'rhu_var9': [90, 90, 395, 385, 90, 95],  # Minimum relative humidity
    'pre_var8': [4.0, 32766, 1.0, 2.0, 1.0, 0.0],  # Evening precipitation
    'pre_var9': [10, 15, 32700, 0.5, 31110, 0.5],  # Morning precipitation
    'pre_var10': [32001, 15, 15, 2.0, 32700, 1.0],  # Daily precipitation
    'tem_var11': [0, 0, 9, 9, 0, 0],  # Quality control code for tem_var8
    'tem_var12': [9, 0, 0, 0, 9, 0],  # Quality control code for tem_var9
    'tem_var13': [0, 9, 0, 0, 0, 9],  # Quality control code for tem_var10
    'rhu_var10': [0, 0, 0, 0, 0, 0],  # Quality control code for rhu_var8
    'rhu_var11': [0, 0, 0, 0, 0, 0],  # Quality control code for rhu_var9
    'pre_var11': [0, 9, 0, 0, 0, 0],  # Quality control code for pre_var8
    'pre_var12': [0, 0, 0, 0, 0, 0],  # Quality control code for pre_var9
    'pre_var13': [0, 0, 0, 0, 0, 0]  # Quality control code for pre_var10
})

# Save the DataFrame to a .dta file
data.to_stata('D:\projects\weather/test\sample.dta', write_index=False)

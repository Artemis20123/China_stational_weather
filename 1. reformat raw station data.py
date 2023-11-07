import os
import pandas as pd

# Function to convert degrees minute to decimal degrees
def dms_to_dd(dms):
    degrees = int(dms) // 100
    minutes = (int(dms) % 100) / 60
    return degrees + minutes


# Function to reformat and save the files by each day
def reformat_file(input_file, output_dir):
    # Read the input file
    df = pd.read_table(input_file, sep=r"\s+", header=None, names=range(10), index_col=False,
                       usecols=[0, 1, 2, 4, 5, 6, 7, 8, 9])

    # Convert longitude and latitude to decimal degrees
    df[2] = df[2].apply(dms_to_dd)
    df[1] = df[1].apply(dms_to_dd)

    # Convert temperature units from 0.1℃ to 1℃
    for col in [7, 8, 9]:
        df[col] = df[col] / 10

    # Rename columns
    df.columns = ["stationID", "LAT", "LON", "year", "month", "day", "avg_temp", "high_temp", "low_temp"]

    # Group by year, month, and day and save the files for each day, facilitating further interpolation
    for _, day_data in df.groupby(["year", "month", "day"]):
        output_file = os.path.join(output_dir,
                                   f"{day_data['year'].iloc[0]}-{day_data['month'].iloc[0]}-{day_data['day'].iloc[0]}.csv")
        day_data.to_csv(output_file, index=False)
        print(f"Processed: {output_file}")


# Input folder and output folder paths
# input_folder = "D:\weather\SURF_CLI_CHN_MUL_DAY_V3.0\datasets\TEM"
input_folder = "../weather/raw data/SURF_CLI_CHN_MUL_DAY_V3.0/datasets/TEM"

# output_folder = "D:\weather\SURF_CLI_CHN_MUL_DAY_V3.0\processed\splitday"
output_folder = "../weather/raw data/SURF_CLI_CHN_MUL_DAY_V3.0\processed\splitday"

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Process each file in the input folder
for filename in os.listdir(input_folder):
    input_file = os.path.join(input_folder, filename)
    reformat_file(input_file, output_folder)

print("Reformatting complete!")

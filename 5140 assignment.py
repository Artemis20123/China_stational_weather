import pandas as pd

# Read the Stata file
data = pd.read_stata('D:\OneDrive - HKUST (Guangzhou)\courses/5140\Expirical Exercise 2\city_year.dta')

# Group by the city code and compute correlation coefficients
corr_df = data.groupby('year').apply(lambda x: x['pm10'].corr(x['missing'])).reset_index()
corr_df.columns = ['year', 'coefficient']

# Print the resulting dataframe
corr_df.to_stata('D:\OneDrive - HKUST (Guangzhou)\courses/5140\Expirical Exercise 2\coefficient_year.dta')
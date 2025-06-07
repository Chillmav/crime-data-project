import pandas as pd


df = pd.read_excel("C:\\Users\\Chillmaw\\Desktop\\projects\\crime-data-project\\include\\2020-homeless-count-data-by-census-tract.xlsx", nrows=0)

print(df.columns.tolist())
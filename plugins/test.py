import pandas as pd
from datetime import datetime
import numpy as np
from klasyfikuj_typ_przestepstwa import klasyfikuj_typ_przestepstwa
from klasyfikuj_rodzaj_broni import klasyfikuj_rodzaj_broni
from klasyfikuj_status import klasyfikuj_status

data = pd.read_csv("C:/Users/Chillmaw/Desktop/projects/crime-data-project/include/_extracted_crime_data.csv")

columns=["id_szczegoly_przestepstwa", "typ", "rodzaj_broni", "status"]
rows = []
index = 1
for index, row in data.iterrows():

    rows.append({
        "id_szczegoly_przestepstwa": index,
        "typ": klasyfikuj_typ_przestepstwa(row["Crm Cd Desc"]),
        "rodzaj_broni": klasyfikuj_rodzaj_broni(row["Weapon Desc"]),
        "status": klasyfikuj_status(row["Status Desc"])
    })

    index += 1
    
crime_details_dim = pd.DataFrame(rows, columns=columns)
crime_details_dim = crime_details_dim.drop_duplicates(subset=["typ", "rodzaj_broni", "status"]).reset_index(drop=True)

print(crime_details_dim)
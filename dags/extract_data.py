from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd



@dag(start_date=datetime(2025, 6, 7), 
     catchup=False, 
     default_args={"owner": "Astro", "retries": 3},
     schedule ='@monthly', 
     tags=["ETL"])


def etl():

    # Extraction:
    @task

    def extract_crime():
        crime_df = pd.read_csv("/usr/local/airflow/include/Crime_Data_from_2020_to_Present_20250607.csv", usecols=["Vict Age", "Vict Sex", "Vict Descent", "Date Rptd", "DATE OCC", "Weapon Used Cd", "AREA NAME", "Rpt Dist No", "LOCATION", "Crm Cd Desc", "Weapon Desc", "Status Desc"]) # crime data
        print('success')
        return crime_df

    @task
    def extract_homeless():
        homeless_df = pd.read_excel("/usr/local/airflow/include/2020-homeless-count-data-by-census-tract.xlsx", usecols=["City","Community_Name","totPeople"]) # homeless data
        print('success')
        return homeless_df
    
    @task
    def transform_to_dim_bezdomni(data):
        bezdomni = pd.DataFrame(columns=['id_bezdomni', "liczba_bezdomnych", "poz_bezdomnosci"])
        bezdomni["liczba_bezdomnych"] = data["to_people"]
        bezdomni["id_bezdomni"] = bezdomni.index + 1
        bezdomni["poz_bezdomnosci"] = "niski"
        bezdomni["poz_bezdomnosci"].where(bezdomni["poz_bezdomnosci"] > 2000, "średni")
        bezdomni["poz_bezdomnosci"].where(bezdomni["poz_bezdomnosci"] > 10000, "duży")
        return bezdomni

    @task
    def transform_to_dim_terytorium(data):
        terytorium = data["AreaName", "Rpt Dist No", "LOCATION"]
        terytorium.dropna()
        terytorium.rename(columns={"AreaName": "Obszar", "Rpt Dist No": "Dzielnica", "LOCATION": "Ulica"})
        terytorium.drop_duplicates(inplace=True)
        area_dist = terytorium["Obszar", "Dzielnica"].drop_duplicates()
        code_to_name = {}
        terytorium["Dzielnica"] = terytorium["Dzielnica"].map(code_to_name)
        area_dist.insert(2, "Ulica", 'nieznana')
        area = terytorium["Obszar"].drop_duplicates()
        area.insert(1, "Dzielnica", "nieznana")
        area.insert(2,"Ulica", "nieznana")
        terytorium = pd.concat([terytorium, area_dist, area, pd.DataFrame(["nieznany", "nieznana", "nieznana"], columns=terytorium.columns)])
        return terytorium

    @task
    def transform_to_facts():
        
        pass
    
    crime_data = extract_crime()
    homeless_data = extract_homeless()
    dim_bezdomni = transform_to_dim_bezdomni(homeless_data)
    dim_terytorium = transform_to_dim_terytorium(crime_data)
    facts = transform_to_facts(crime_data)

etl()







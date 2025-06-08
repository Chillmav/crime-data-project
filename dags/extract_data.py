from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from plugins.klasyfikuj_typ_przestepstwa import klasyfikuj_typ_przestepstwa
from plugins.klasyfikuj_rodzaj_broni import klasyfikuj_rodzaj_broni
from plugins.klasyfikuj_status import klasyfikuj_status


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

        return crime_df

    @task

    def extract_homeless():

        homeless_df = pd.read_excel("/usr/local/airflow/include/2020-homeless-count-data-by-census-tract.xlsx", usecols=["City","Community_Name","totPeople"]) # homeless data

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

    @task
    def create_dim_data():

        
        columns = ["id_data", "rok", "miesiac", "tydzień", "kwartał", "dzień_roboczy", "pora_dnia"]
        rows = []

        date_range = pd.date_range(start="2020-01-01", end=datetime.today(), freq='D')

        dni_po_polsku = {
            'Monday': 'poniedziałek',
            'Tuesday': 'wtorek',
            'Wednesday': 'środa',
            'Thursday': 'czwartek',
            'Friday': 'piątek',
            'Saturday': 'sobota',
            'Sunday': 'niedziela'
        }

        pory_dnia = ['rano', 'południe', 'wieczór', 'noc']

        id_counter = 1
        for date in date_range:
            for pora in pory_dnia:
                rows.append({
                    "id_data": id_counter,
                    "rok": date.year,
                    "miesiac": date.month,
                    "tydzień": date.isocalendar().week,
                    "kwartał": (date.month - 1) // 3 + 1,
                    "dzień_roboczy": dni_po_polsku[date.day_name()],
                    "pora_dnia": pora
                })
                id_counter += 1

        data = pd.DataFrame(rows, columns=columns)


        return data

    @task
    def create_dim_ofiara():

        columns = ["id_ofiary", "wiek", "płeć", "rasa"]
        rows = []
        index = 1
        for age in range(0, 101):
            for sex in ['m', 'f', 'nieznana']:
                for race in ["A", "B", "C", "D", "F", "G", "H", "I", "J", "K", "L", "O", "P", "S", "U", "V", "W", "X", "Z"]:
                    rows.append({
                        "id_ofiary": index,
                        "wiek": age,
                        "płeć": sex,
                        "rasa": race
                    })

                    index += 1

        data = pd.DataFrame(rows, columns=columns)

        return data

    @task
    def transform_to_dim_szczegoly_przestepstwa(data):

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
        

        return crime_details_dim

    crime_data = extract_crime()
    homeless_data = extract_homeless()
    dim_bezdomni = transform_to_dim_bezdomni(homeless_data)
    dim_terytorium = transform_to_dim_terytorium(crime_data)
    facts = transform_to_facts(crime_data)

etl()







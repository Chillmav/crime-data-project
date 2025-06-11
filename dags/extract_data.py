from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from plugins.klasyfikuj_typ_przestepstwa import klasyfikuj_typ_przestepstwa
from plugins.klasyfikuj_rodzaj_broni import klasyfikuj_rodzaj_broni
from plugins.klasyfikuj_status import klasyfikuj_status
from plugins.mapowanie_obszarow import community_to_area


@dag(start_date=datetime(2025, 6, 7), 
     catchup=False, 
     default_args={"owner": "Astro", "retries": 3},
     schedule ='@monthly', 
     tags=["ETL"])


def etl():

    # Extraction:
    @task

    def extract_crime():

        crime_df = pd.read_csv("/usr/local/airflow/include/Crime_Data_from_2020_to_Present_20250607.csv", usecols=["Vict Age", "Vict Sex", "Vict Descent", "Date Rptd", "DATE OCC", "Time OCC", "Weapon Used Cd", "AREA NAME", "Rpt Dist No", "LOCATION", "Crm Cd Desc", "Weapon Used Cd", "Weapon Desc", "Status Desc"]) # crime data

        return crime_df

    @task

    def extract_homeless():

        homeless_df = pd.read_excel("/usr/local/airflow/include/2020-homeless-count-data-by-census-tract.xlsx", sheet_name="Counts_by_Tract", usecols=["City","Community_Name","totPeople"]) # homeless data

        return homeless_df
    
    @task
    def transform_to_dim_bezdomni(data):

        bezdomni = pd.DataFrame(columns=['id_bezdomni', "liczba_bezdomnych", "poz_bezdomnosci"])
        
        def extract_primary_area(name):
            for part in name.split('/'):
                part = part.strip()
                if part in community_to_area:
                    return community_to_area[part]
            return None  # or "Unknown"

        data['area'] = data['Community_Name'].apply(extract_primary_area)
        data = data.groupby(["area"], as_index=False)["totPeople"].sum()
        data["totPeople"] = data["totPeople"].round(decimals=0)
        data["totPeople"] = data["totPeople"].astype('Int64')
        bezdomni["liczba_bezdomnych"] = data["totPeople"]
        bezdomni["area"] = data['area']  # area is needed for creating facts table must be dropped before loading to dim table
        bezdomni["id_bezdomni"] = bezdomni.index + 2
        bezdomni["poz_bezdomnosci"] = "niski"
        bezdomni["poz_bezdomnosci"] = bezdomni["poz_bezdomnosci"].where(bezdomni["liczba_bezdomnych"] < 2000, "średni")
        bezdomni["poz_bezdomnosci"] = bezdomni["poz_bezdomnosci"].where(bezdomni["liczba_bezdomnych"] < 5000, "duży")
        bezdomni = pd.concat([pd.DataFrame([[1, 0, "nieznany", "nieznany"]], columns=bezdomni.columns), bezdomni])

        return bezdomni

    @task
    def transform_to_dim_terytorium(data):
        terytorium = data[["AREA NAME", "Rpt Dist No", "LOCATION"]]
        terytorium.dropna()
        terytorium.rename(columns={"AREA NAME": "Obszar", "Rpt Dist No": "Dzielnica", "LOCATION": "Ulica"}, inplace=True)
        terytorium.drop_duplicates(inplace=True)
        area_dist = terytorium[["Obszar", "Dzielnica"]].drop_duplicates()
        area_dist.insert(2, "Ulica", 'nieznana')
        area = terytorium["Obszar"].drop_duplicates()
        area = area.to_frame()
        area.insert(1, "Dzielnica", "nieznana")
        area.insert(2,"Ulica", "nieznana")
        terytorium["id_terytorium"] = terytorium.index + 1
        terytorium = pd.concat([pd.DataFrame([["nieznany", 0, "nieznana", 1]], columns=terytorium.columns), terytorium, area_dist, area])

        
        return terytorium

    @task
    def transform_to_facts(data, dim_ofiara, dim_terytorium, dim_data, dim_sczegoly_przestepstwa, dim_bezdomni):
        # transform dates to year, month, week, day
        data["Date Rptd"] = pd.to_datetime(data["Date Rptd"], format="%m, %d, %y")
        data['rok_rp'] =  pd.DatetimeIndex(data['Date Rptd']).year
        data["kwartal_rp"] = pd.DatetimeIndex(data['Date Rptd']).quarter
        data['miesiac_rp'] = pd.DatetimeIndex(data['Date Rptd']).month     
        data['dzien_rp'] = data['Date Rptd'].dt.day_name()
        data['tydzien_rp'] = data["Date Rptd"].dt.isocalendar().week

        data["Date oc"] =  pd.to_datetime(data["Date OCC"], format="%m, %d, %y")
        data['rok_oc'] =  pd.DatetimeIndex(data['Date CC']).year
        data["kwartal_oc"] = pd.DatetimeIndex(data['Date Rptd']).quarter
        data['miesiac_oc'] =  pd.DatetimeIndex(data['Date OCC']).month
        data['dzien_oc'] =  data['Date OCC'].dt.day_name()
        data['tydzien_oc'] = data["Date OCC"].dt.isocalendar().week
        data["pora_dnia"] = "noc"
        data["pora_dnia"] = data["pora_dnia"].where(data["TIME OCC"] < 500, "rano")
        data["pora_dnia"] = data["pora_dnia"].where(data["TIME OCC"] < 1100, "południe")
        data["pora_dnia"] = data["pora_dnia"].where(data["TIME OCC"] < 1800, "wieczorem")
        data["pora_dnia"] = data["pora_dnia"].where(data["TIME OCC"] < 2300, "noc")

        dni_po_polsku = {
            'Monday': 'poniedziałek',
            'Tuesday': 'wtorek',
            'Wednesday': 'środa',
            'Thursday': 'czwartek',
            'Friday': 'piątek',
            'Saturday': 'sobota',
            'Sunday': 'niedziela'
        }

        data["dzien_oc"] = data["dzien_oc"].map(dni_po_polsku)
        data["dzien_rp"] = data["dzien_rp"].map(dni_po_polsku)

        # calculate time between occ and rptd
        data["czas_zgloszenia"] = data["Date Rptd"] - data["Date OCC"]
        data["czas_zgloszenia"] = data["czas_zgloszenia"].dt.days
        data.drop(columns=["Date Rptd", "Date OCC", "TIME OCC"], inplace=True)

        rows = []
        for index, row in data.iterrows():

            rows.append({
                "id_szczegoly_przestepstwa": index,
                "typ": klasyfikuj_typ_przestepstwa(row["Crm Cd Desc"]),
                "rodzaj_broni": klasyfikuj_rodzaj_broni(row["Weapon Desc"]),
                "status": klasyfikuj_status(row["Status Desc"])
            })

            index += 1

        data[["Crm Cd Desc", "Weapon Desc", "Status Desc"]] = rows

        data.rename({"Crm Cd Desc":"typ", "Weapon Desc":"rodzaj_broni", "Status Desc":"status"}, inplace=True)

        danger_dict = {
            "Zabójstwa":5,
            "Kradzież":2,
            "Przemoc fizyczna":4,
            "Przemoc seksualna":4,
            "Przestępstwa narkotykowe":3,
            "Przestępstwa z użyciem broni":4,
            "Zakłócanie porządku":1,
            "Inne":3
        }

        # calculate danger level
        data['st_niebezpieczenstwa'] = data['Weapon Used Cd'].astype(str).str[0].astype(int)
        data['st_niebezpieczenstwa'].fillna(0)
        data["typ_nr"] = data["typ"].map(danger_dict)
        data['st_niebezpieczenstwa'] = data['st_niebezpieczenstwa'] + data["typ_nr"]
        data.drop(columns=["typ_nr", "Weapon Used Cd"], inplace=True)

        ethnicity_dict = {
            "A": "Inny Azjata",
            "B": "Czarny",
            "C": "Chińczyk",
            "D": "Kambodżanin",
            "F": "Filipińczyk",
            "G": "Guamczyk",
            "H": "Latynos/ Latynoska/ Meksykanin",
            "I": "Indianin amerykański/ rdzenny mieszkaniec Alaski",
            "J": "Japończyk",
            "K": "Koreańczyk",
            "L": "Laotańczyk",
            "O": "Inny",
            "P": "Mieszkaniec wysp Pacyfiku",
            "S": "Samoańczyk",
            "U": "Hawajczyk",
            "V": "Wietnamczyk",
            "W": "Biały",
            "X": "Nieznany",
            "Z": "Indianin azjatycki"
        }

        data["Vict Descent"] = data["Vict Descent"].map(ethnicity_dict)
        data["Vict Sex"] = data["Vict Sex"].where(data["Vict Sex"].isin(["M", "F"]), "nieznana")
        
        data.rename({"Vict Descent": "rasa", "Vict Age": "wiek", "Vict Sex":"plec"}, inplace=True)
        
        # create fact table
        facts = pd.DataFrame(columns=["czas_zgloszenia", "st_niebezpieczenstwa", "id_data_zp", "id_data_pp", "id_terytorium", "id_ofiara", "id_szcegoly_przestepstwa", "id_bezdomni"])
        facts["czas_zgloszenia"] = data["czas_zgloszenia"]
        facts["st_niebezpieczenstwa"] = data["st_niebezpieczenstwa"]

        merged_df = pd.merge(data[["rok_rp", "miesiac_rp", "dzien_rp"]], dim_data[["id_data", "rok", "miesiac", "dzień"]], left_on=["rok_rp", "miesiac_rp", "dzien_rp"], right_on=["rok", "miesiac", "dzień"], how='left')
        merged_df["id_data"].fillna(1)
        facts["id_data_zp"] = merged_df["id_data"]
        merged_df = pd.merge(data[["rok_oc", "miesiac_rp_oc", "dzien_oc"]], dim_data[["id_data", "rok", "miesiac", "dzień", "pora_dnia"]], left_on=["rok_oc", "miesiac_oc", "dzien_oc", "pora_dnia"], right_on=["rok", "miesiac", "dzień", "pora_dnia"], how='left')
        merged_df["id_data"].fillna(1)
        facts["id_data_pp"] = merged_df["id_data"]
        merged_df = pd.merge(data[[["wiek", "plec", "rasa"]]], dim_ofiara[["id_ofiary", "wiek", "płeć", "rasa"]], left_on=[[ "wiek", "plec", "rasa"]], right_on=[["wiek", "płeć", "rasa"]], how='left')
        merged_df["id_ofiary"].fillna(1)
        facts["id_ofiary"] = merged_df["id_ofiary"]
        merged_df = pd.merge(data[["typ", "rodzaj_broni", "status"]], dim_sczegoly_przestepstwa[["id_szczegoly_przestepstwa", "typ", "rodzaj_broni", "status"]], left_on=[[ "typ", "rodzaj_broni", "status"]], right_on=[["typ", "rodzaj_broni", "status"]], how='left')
        merged_df["id_szcegoly_przestepstwa"].fillna(1)
        facts["id_szcegoly_przestepstwa"] = merged_df["id_szcegoly_przestepstwa"]
        merged_df = pd.merge(data[["AREA NAME"]], dim_bezdomni[["id_bezdomni", "area"]], left_on=["AREA NAME"], right_on=["area"], how='left')
        merged_df["id_bezdomni"].fillna(1)
        facts["id_bezdomni"] = merged_df["id_bezdomni"]
        merged_df = pd.merge(data[["AREA NAME", "Rpt Dist No", "LOCATION"]], dim_terytorium[["id_terytorium", "Obszar", "Dzielnica", "Ulica"]], left_on=[["AREA NAME", "Rpt Dist No", "LOCATION"]], right_on=[["Obszar", "Dzielnica", "Ulica"]], how='left')
        merged_df["id_terytorium"].fillna(1)
        facts["id_terytorium"] = merged_df["id_terytorium"]

        return facts

    @task
    def create_dim_data():

        columns = ["id_data", "rok", "miesiac", "tydzień", "kwartał", "dzień", "dzień_roboczy", "pora_dnia"]
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

        id_counter = 2
        for date in date_range:
            for pora in pory_dnia:
                rows.append({
                    "id_data": id_counter,
                    "rok": date.year,
                    "miesiac": date.month,
                    "tydzień": date.isocalendar().week,
                    "kwartał": (date.month - 1) // 3 + 1,
                    "dzień": dni_po_polsku[date.day_name()],
                    "dzień_roboczy": "False", # funkcja do sprawdzenia
                    "pora_dnia": pora
                })
                id_counter += 1

        data = pd.DataFrame(rows, columns=columns)
        data = pd.concat([pd.DataFrame([[1, 0, 0, 0, 0, "nieznany", "nieznana", "nieznana"]], columns=data.columns), data])


        return data

    @task
    def create_dim_ofiara():
        columns = ["id_ofiary", "wiek", "płeć", "rasa"]
        rows = []
        index = 1
        for age in range(0, 101):
            for sex in ['M', 'F', 'nieznana']:
                for race in [
                    "Inny Azjata",
                    "Czarny",
                    "Chińczyk",
                    "Kambodżanin",
                    "Filipińczyk",
                    "Guamczyk",
                    "Latynos/ Latynoska/ Meksykanin",
                    "Indianin amerykański/ rdzenny mieszkaniec Alaski",
                    "Japończyk",
                    "Koreańczyk",
                    "Laotańczyk",
                    "Inny",
                    "Mieszkaniec wysp Pacyfiku",
                    "Samoańczyk",
                    "Hawajczyk",
                    "Wietnamczyk",
                    "Biały",
                    "Nieznany",
                    "Indianin azjatycki"
                ]:
                    rows.append({
                        "id_ofiary": index,
                        "wiek": age,
                        "płeć": sex,
                        "rasa": race
                    })

                    index += 1

        data = pd.DataFrame(rows, columns=columns)
        data = pd.concat([pd.DataFrame([[1, 0, "nieznana", "nieznana"]], columns=data.columns), data])

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
        crime_details_dim = pd.concat([pd.DataFrame([[1, "nieznany", "nieznany", "nieznany"]], columns=crime_details_dim.columns), crime_details_dim])

        return crime_details_dim

    crime_data = extract_crime()
    homeless_data = extract_homeless()
    dim_ofiara = create_dim_ofiara()
    dim_data = create_dim_data()
    dim_sczegoly_przestepstwa = transform_to_dim_szczegoly_przestepstwa(crime_data)
    dim_bezdomni = transform_to_dim_bezdomni(homeless_data)
    dim_terytorium = transform_to_dim_terytorium(crime_data)
    facts = transform_to_facts(crime_data, dim_ofiara, dim_terytorium, dim_data, dim_sczegoly_przestepstwa, dim_bezdomni)

etl()







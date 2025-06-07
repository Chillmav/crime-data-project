from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd







@dag(start_date=datetime(2025, 6, 7), catchup=False)

def extract_dag():

    @task

    # Extraction:

    def extract_data():

        crime_df = pd.read_csv("/usr/local/airflow/include/Crime_Data_from_2020_to_Present_20250607.csv", usecols=["Vict Age", "Vict Sex", "Vict Descent", "Date Rptd", "DATE OCC", "Weapon Used Cd", "AREA NAME", "Rpt Dist No", "LOCATION", "Crm Cd Desc", "Weapon Desc", "Status Desc"]) # crime data
        crime_df.to_csv("/usr/local/airflow/include/_extracted_crime_data.csv", index=False)
        print('success')

        homeless_df = pd.read_excel("/usr/local/airflow/include/2020-homeless-count-data-by-census-tract.xlsx", usecols=["City","Community_Name","totPeople"]) # homeless data
        homeless_df.to_csv("/usr/local/airflow/include/_extracted_homeless_data.csv", index=False)
        print('success')
    
    extract_data()



extract_dag = extract_dag()

# Victim:





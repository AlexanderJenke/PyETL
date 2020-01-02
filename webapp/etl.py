from db import ConfigHandler
from pandas import DataFrame, read_csv
import matplotlib.pyplot as plt
import pandas as pd 
import os

handler = ConfigHandler()

def insert_into_FALL(csv):
    frame = read_csv(csv)


def get_row_count_from_csv(csv):
    df = read_csv(csv, delimiter=";")
    return len(df)

def run_etl_process(csv_files):
    csv_is_updataed = False
    for csv in csv_files:
        print(csv)
        last_row_count = handler.get_row_count(csv)
        current_row_count = get_row_count_from_csv(csv)
        if last_row_count != current_row_count:
            csv_is_updated = True
        handler.update_row_count(csv, str(current_row_count))
    if not csv_is_updated:
        print("CSV files were not changed!")
        return
    print("Now, the ETL process should start")
    fr csv in csv_files:
        if csv.endswith("FALL.csv"):
            insert_into_FALL(csv)
    

if __name__ == "__main__":
    csv_dir = handler.get_csv_dir()
    csv_files = list(filter(lambda file_name: file_name.endswith(".csv"), os.listdir(csv_dir)))
    csv_files = [(csv_dir + "/" + i) for i in csv_files]
    print(csv_files)
    run_etl_process(csv_files)
    

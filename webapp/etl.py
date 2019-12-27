from db import ConfigHandler
from pandas import DataFrame, read_csv
import matplotlib.pyplot as plt
import pandas as pd 

def get_row_count_from_csv(csv):
    df = read_csf(csv)
    return len(df)

def run_etl_process(csv_files):
    handler = ConfigHandler()
    csv_is_updataed = False
    for csv in csv_files:
        last_row_count = handler.get_row_count(csv)
        current_row_count = get_row_count_from_csv(csv)
        if last_row_count != current_row_count:
            csv_is_updated = True
        handler.update_row_count(csv, current_row_count)
    if not csv_is_updated:
        print("CSV files were not changed!")
        return
    print("Now, the ETL process should start")

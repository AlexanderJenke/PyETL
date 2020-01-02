import pandas as pd
from sys import argv
import os

if __name__ == "__main__":
    csv_dir = argv[1]
    
    fall_csv = os.path.join(csv_dir, "FALL.csv")
    icd_csv = os.path.join(csv_dir,  "ICD.csv")
    messungen_csv = os.path.join(csv_dir, "MESSUNGEN.csv")
    labor_csv = os.path.join(csv_dir, "LABOR.csv")
    ops_csv = os.path.join(csv_dir, "OPS.csv")

    fall_pd = pd.read_csv(fall_csv)
    icd_pd = pd.read_csv(icd_csv)
    messungen_csv = pd.read_csv(messungen_csv)
    labor_csv = pd.read_csv(labor_csv)
    ops_csv = pd.read_csv(ops_csv)

    person_d = {}
    

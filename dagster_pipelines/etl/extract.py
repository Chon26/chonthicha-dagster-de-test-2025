import pandas as pd

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel() -> pd.DataFrame:
    df = pd.read_excel("dagster_pipelines/data/KPI_FY.xlsm", sheet_name="Data to DB")

    # ทำการแปลงคอลัมน์ Center_ID เป็น string เพื่อแก้ไขปัญหาการ merge ที่เกิดจากคอลัมน์ Center_ID เป็น object
    df['Center_ID'] = df['Center_ID'].astype(str)
    
    return df
    

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv() -> pd.DataFrame:
    df = pd.read_csv("dagster_pipelines/data/M_Center.csv")

    # ทำการแปลงคอลัมน์ Center_ID เป็น string เพื่อแก้ไขปัญหาการ merge ที่เกิดจากคอลัมน์ Center_ID เป็น object
    df['Center_ID'] = df['Center_ID'].astype(str)

    return df
    
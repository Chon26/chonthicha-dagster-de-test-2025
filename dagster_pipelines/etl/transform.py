import pandas as pd
from dagster_pipelines.etl.extract import read_excel
# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data() -> pd.DataFrame:
    df = read_excel()
    
    # ระบุคอลัมน์ที่ไม่ใช่ Plan_ หรือ Actual_
    id_vars = [col for col in df.columns if not col.split('_')[0] in ['Plan', 'Actual']]
    
    # Melt ข้อมูลจาก wide เป็น long format
    df_melted = df.melt(id_vars=id_vars, var_name='Amount Name', value_name='Amount')
    
    # สร้างคอลัมน์ Amount Type
    df_melted['Amount Type'] = df_melted['Amount Name'].str.split('_').str[0]
    
    # จัดลำดับคอลัมน์ใหม่: นำ Amount Type ไว้หน้าคอลัมน์ Amount Name
    cols = []
    for col in df_melted.columns:
        if col == 'Amount Name':
            cols.append('Amount Type')
            cols.append('Amount Name')
        elif col != 'Amount Type':
            cols.append(col)
    
    df_melted = df_melted[cols]
    
    return df_melted
import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb
import pandas as pd
from datetime import datetime
# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext)  -> pd.DataFrame :
    df = pivot_data()
    load_to_duckdb(df,'KPI_FY')
    return df


# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan",ins={'kpi_fy':dg.AssetIn('kpi_fy')})
def m_center(context: dg.AssetExecutionContext,kpi_fy:pd.DataFrame) -> pd.DataFrame :
    df = read_csv()
    load_to_duckdb(df,'M_Center')
    return df


# 2.3.2 Create asset kpi_fy_final_asset()
@dg.asset(compute_kind="duckdb", group_name="plan",ins={'kpi_fy':dg.AssetIn('kpi_fy'),'m_center':dg.AssetIn('m_center')})
def kpi_fy_final_asset(context: dg.AssetExecutionContext, kpi_fy:pd.DataFrame,m_center:pd.DataFrame) -> pd.DataFrame :
    df = kpi_fy
    df2 = m_center
    df_final = df.merge(df2, on='Center_ID' , how='left')
    df_final['updated_at'] = datetime.now()
    load_to_duckdb(df_final,'KPI_FY_Final')
    return df_final

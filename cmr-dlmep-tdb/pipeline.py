import json
import typing
import tempfile
import io
import os

import pandas as pd
import geopandas as gpd
import papermill as pm

from datetime import date, datetime
from shapely.geometry import shape

from openhexa.sdk import current_run, parameter, pipeline, workspace

try:
    from openhexa.toolbox.dhis2 import DHIS2
except ModuleNotFoundError:
    os.system('pip install openhexa.toolbox')
    
    from openhexa.toolbox.dhis2 import DHIS2

try:
    from epiweeks import Week, Year
except ModuleNotFoundError:
    os.system('pip install epiweeks')

    from epiweeks import Week, Year

@pipeline("cmr-dlmep-tdb", name="CMR DLMEP TdB")
@parameter(
    "get_year",
    name="Year",
    help="Year for which to extract and process data",
    type=int,
    default=2023,
    required=False,
)
@parameter(
    "get_download",
    name="Download new data?",
    help="Download new data or reprocess existing extracts",
    type=bool,
    default=True,
    required=False,
)
@parameter(
    "get_upload",
    name="Upload?",
    help="Whether or not to push the processed results to the dashboard DB",
    type=bool,
    default=False,
    required=False,
)
def dlmep_extract_process(get_year, get_upload, get_download, *args, **kwargs):
    """
    MAPE data element group : dWpQSgPS6bw
    """

    # setup variables
    PROJ_ROOT = f'{workspace.files_path}/dlmep-tdb/'
    DATA_DIR = f'{PROJ_ROOT}data/'
    RAW_DATA_DIR = f'{DATA_DIR}raw/'

    INPUT_NB = f'{PROJ_ROOT}MAPE_CMR_process.ipynb'
    OUTPUT_NB_DIR = f'{PROJ_ROOT}papermill-outputs/'

    if get_download:
        # extract data from DHIS
        dhis_download_complete = dhis2_download(RAW_DATA_DIR, get_year)

    # run processing code in notebook
    params = {'ANNEE': get_year, 'UPLOAD': get_upload}

    if get_download:
        ppml = run_papermill_script(INPUT_NB, OUTPUT_NB_DIR, params, dhis_download_complete)
    else:
        ppml = run_papermill_script(INPUT_NB, OUTPUT_NB_DIR, params)

@dlmep_extract_process.task
def dhis2_download(output_dir, year, *args, **kwargs):
    
    # establish DHIS2 connection
    connection = workspace.dhis2_connection('CMR_SNIS')
    dhis2 = DHIS2(connection=connection, cache_dir = None) # f'{workspace.files_path}/temp/')

    current_run.log_info(f"Connected to {connection.url}")

    monitored_deg = ["dWpQSgPS6bw"]
    periods = get_dhis_epiweeks(year)

    current_run.log_info(f"Extracting instance analytics data for {year}")

    raw_data = dhis2.analytics.get(
        data_element_groups = monitored_deg,
        periods = periods,
        org_unit_levels = [3]
    )

    df = pd.DataFrame(raw_data)

    current_run.log_info("Extracting + merging instance metadata")
    df = dhis2.meta.add_org_unit_name_column(dataframe=df)
    df = dhis2.meta.add_org_unit_parent_columns(dataframe=df)
    df = dhis2.meta.add_dx_name_column(dataframe=df)
    df = dhis2.meta.add_coc_name_column(dataframe=df)

    current_run.log_info("Creating analytics.csv")

    os.makedirs(f'{output_dir}/{year}/', exist_ok=True)

    out_path = f'{output_dir}/{year}/analytics.csv'
    df.to_csv(out_path)

    return out_path

@dlmep_extract_process.task
def run_papermill_script(in_nb, out_nb_dir, parameters, *args, **kwargs):
    current_run.log_info(f"Running code in {in_nb}")

    execution_timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
    out_nb = f"{out_nb_dir}{os.path.basename(in_nb)}_OUTPUT_{execution_timestamp}.ipynb"

    return pm.execute_notebook(in_nb, out_nb, parameters)


#### helper functions ####
def get_dhis_epiweeks(year):
    """ 
    Returns a list of DHIS2 weeks (yyyyWn -- 2020W10) based on the year
    specified to the function. For the current year, all weeks up to N-1 are 
    included in the list. For previous years, the list contains all weeks.
    """

    current_date = date.today()
    
    # current year : up to last week
    if year == current_date.year:
        current_epi_week = Week.fromdate(current_date).week
        
        week_list = [f'{year}W{x}' for x in range(1, current_epi_week)]
    
    # previous years: all weeks    
    else:
        week_list = [f'{year}W{x.week}' for x in Year(year).iterweeks()]
        
    return week_list

if __name__ == "__main__":
    dlmep_extract_process.run()

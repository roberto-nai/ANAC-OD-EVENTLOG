# 02_log_filter.py
# Filters the event log keeping only the case-ids (CIG) present in TED texts

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path
import csv

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import df_read_csv, df_print_details, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
csv_sep = str(yaml_config["CSV_FILE_SEP"])

stats_dir =  str(yaml_config["OD_STATS_DIR"])

log_dir =  str(yaml_config["EVENT_LOG_DIR"])

file_event_log = "anac_log_2016_2022.csv" # INPUT: the main event log

file_cig_ted = "ANAC_TED_CIG_found.csv" # INPUT: CIG in TED texts

file_event_log_ted = "anac_log_2016_2022_ted.csv" # OUTPUT: the event log with cases from TED texts (PDFs) get by CIG

script_path, script_name = script_info(__file__)

### FUNCTIONS ###

def filter_cases_by_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the event log DataFrame to include only the cases where the first event is 'TENDER_NOTICE'
    and the last event is 'CONTRACT_END'.
    
    Parameters:
    df (pd.DataFrame): A DataFrame representing the event log, with at least the following columns:
                       - 'case_id': Unique identifier for each case.
                       - 'event_name': Name of the event (e.g., 'TENDER_NOTICE', 'CONTRACT_END').
                       - 'event_timestamp': Timestamp of the event (used for sorting events within a case).
    
    Returns:
    pd.DataFrame: A filtered DataFrame containing only the cases that meet the criteria.
    """
    # Sort the DataFrame by 'case_id' and 'event_timestamp' to ensure events are in chronological order
    df_sorted = df.sort_values(by=['case_id', 'event_timestamp'])

    # Group the DataFrame by 'case_id' and extract the first and last event for each case
    first_last_events = df_sorted.groupby('case_id').agg(
        first_event=('event_name', 'first'),
        last_event=('event_name', 'last')
    )

    # Filter cases where the first event is 'TENDER_NOTICE' and the last event is 'CONTRACT_END'
    filtered_cases = first_last_events[
        (first_last_events['first_event'] == 'TENDER_NOTICE') & 
        (first_last_events['last_event'] == 'CONTRACT_END')
    ].index

    # Filter the original DataFrame to include only the selected cases
    df_filtered = df[df['case_id'].isin(filtered_cases)]
    
    df_filtered = df_filtered.sort_values(by=['case_id', 'event_timestamp'])

    # Return the filtered DataFrame
    return df_filtered

### MAIN ###

def main():
    print()
    print(f"*** PROGRAM START ({script_name}) ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process: " + str(start_time))
    print()

    print(">> Reading complete event log")
    list_col_exc = []
    list_col_type_dic = {"case_id":object,"event_name":object,"event_timestamp":object,"oggetto_principale_contratto":object, "importo_lotto":float, "accordo_quadro":object,"cpv_division":object,"sezione_regionale":object,"cod_tipo_scelta_contraente":object,"cod_modalita_realizzazione":object,"case_len":int}

    df_log = df_read_csv(log_dir, file_event_log, list_col_exc, list_col_type_dic, None, csv_sep)
    df_print_details(df_log, f"File '{file_event_log}'")
    print()

    print("Regions inf event log:", df_log["sezione_regionale"].unique())
    print()

    """
    print(">> Filtering event log by events (initial and final)")
    df_log = filter_cases_by_events(df_log)
    df_log = df_log.sort_values(by=['case_id', 'event_timestamp']).reset_index(drop=True)
    print("Shape of the event log after event filters:", df_log.shape)
    print("Cases in the event log after event filters:", df_log["case_id"].nunique())
    df_print_details(df_log, f"File '{file_event_log}'")
    print()
    path_log_out = Path(log_dir) / file_event_log_ted   # <-- output (debug)
    df_log.to_csv(path_log_out, sep=";", index=False)
    """
    
    # Data from TED
    print(">> Reading CIGs in TED")
    path_cig_ted = Path(log_dir) / file_cig_ted
    dic_t = {"cig_ted":object}
    df_cig_ted = pd.read_csv(path_cig_ted, dtype=dic_t)
    df_print_details(df_cig_ted, f"File '{file_event_log_ted}'")
    print()

    list_cig_ted = df_cig_ted["cig_ted"].tolist()
    print("CIGs in list:", len(list_cig_ted))

    # Keep only cases from TED
    print(">> Filtering event log by events (initial and final)")
    df_log_ted = df_log[df_log['case_id'].isin(list_cig_ted)]
    df_log_ted = df_log_ted.sort_values(by=['case_id', 'event_timestamp']).reset_index(drop=True)
    df_print_details(df_log_ted, f"Filtered")
    print("Filtere vent log cases:", df_log_ted["case_id"].nunique())
    print()

    # Save
    path_anac_ted = Path(log_dir) / file_event_log_ted
    print("Saving filtered event log to:", path_anac_ted)
    df_log_ted.to_csv(path_anac_ted, sep=";", index=False)

    # Program end
    end_time = datetime.now().replace(microsecond=0)
    delta_time = end_time - start_time
    
    print()
    print("End process:", end_time)
    print("Time to finish:", delta_time)
    print()

    print()
    print("*** PROGRAM END ***")
    print()


if __name__ == "__main__":
    main()
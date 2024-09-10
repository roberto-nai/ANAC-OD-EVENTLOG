# 03_log_filter_threshold.py
# Starting from filtered TED event log, extract stas and data above above and below threshold

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path
import csv
from dateutil.relativedelta import relativedelta

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import df_read_csv, df_print_details, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
csv_sep = str(yaml_config["CSV_FILE_SEP"])

stats_dir =  str(yaml_config["OD_STATS_DIR"])

log_dir =  str(yaml_config["EVENT_LOG_DIR"])

file_event_log_ted = "anac_log_2016_2022_ted.csv" # INPUT: the cases from TED texts (PDFs)

script_path, script_name = script_info(__file__)

### FUNCTIONS ###
def filter_df_by_region_and_amount(df:pd.DataFrame, region_list:list, amount: float) -> pd.DataFrame:
    df_log_3_r = df[df['sezione_regionale'].isin(region_list)]
    df_log_3_a = df_log_3_r[df_log_3_r['importo_lotto'] > amount]
    df_log_3_b = df_log_3_r[df_log_3_r['importo_lotto'] <= amount]
    return df_log_3_a, df_log_3_b

def calculate_case_statistics(df_log: pd.DataFrame, output_csv: str) -> None:
    """
    Calculate the duration of each case in months, and provide statistical analysis (mean, median, std deviation)
    grouped by 'oggetto_principale_contratto'. The results are saved to a CSV file.

    Parameters:
    df_log (pd.DataFrame): The event log dataframe containing 'case_id', 'event_timestamp', and 'oggetto_principale_contratto'.
    output_csv (str): The path to save the resulting statistics CSV file.

    Returns:
    None: The function saves the result to a CSV file and does not return any values.
    """
    
    # Ensure the 'event_timestamp' column is in datetime format
    df_log['event_timestamp'] = pd.to_datetime(df_log['event_timestamp'])

    # Calculate the "start_time" (first timestamp) and "end_time" (last timestamp) for each "case_id"
    case_times = df_log.groupby('case_id').agg(
        start_time=('event_timestamp', 'min'),
        end_time=('event_timestamp', 'max')
    ).reset_index()

    # Merge the new start_time and end_time columns with df_log to retain other information
    df_log = pd.merge(df_log, case_times, on='case_id')

    # Function to calculate the difference in months using relativedelta
    def calculate_duration_in_months(row):
        delta = relativedelta(row['end_time'], row['start_time'])
        return delta.years * 12 + delta.months + delta.days / 30  # Approximation for days

    # Apply the function to calculate the duration in months
    df_log['duration_months'] = df_log.apply(calculate_duration_in_months, axis=1)

    # Check for any negative or zero durations
    if (df_log['duration_months'] <= 0).any():
        print("Warning: There are cases with zero or negative durations.")

    # Print some basic statistics to understand the distribution
    print(df_log['duration_months'].describe())  # This will help us understand the distribution

    # Group by "case_id" and "oggetto_principale_contratto" to get the maximum duration for each case
    case_duration = df_log.groupby(['case_id', 'oggetto_principale_contratto'])['duration_months'].max().reset_index()

    # Group by "oggetto_principale_contratto" to get the required statistics
    stats = case_duration.groupby('oggetto_principale_contratto').agg(
        case_len=('case_id', 'nunique'),  # Count the distinct 'case_id' for each group
        mean_duration=('duration_months', 'mean'),
        median_duration=('duration_months', 'median'),
        std_dev_duration=('duration_months', 'std')
    ).reset_index()

    # Round the calculated statistics to 2 decimal places
    stats = stats.round({'mean_duration': 2, 'median_duration': 2, 'std_dev_duration': 2})

    # Save the final statistics to a CSV file
    stats.to_csv(output_csv, sep=csv_sep, index=False)

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

    df_log = df_read_csv(log_dir, file_event_log_ted, list_col_exc, list_col_type_dic, None, csv_sep)
    df_print_details(df_log, f"File '{file_event_log_ted}'")
    print()

    print("Regions inf event log:", df_log["sezione_regionale"].unique())
    print()

    # Filters above and below threshold
    print(">> Division by above/below threshold")
    print()

    # All regions
    list_regions_all = ["CENTRALE", "LOMBARDIA", "PIEMONTE", "LAZIO", "SICILIA", "VENETO", "TOSCANA", "EMILIA ROMAGNA", "CAMPANIA", "PUGLIA", "SARDEGNA", "LIGURIA", "CALABRIA", "MARCHE", "ABRUZZO", "FRIULI VENEZIA GIULIA", "UMBRIA", "BASILICATA", "PROVINCIA AUTONOMA DI BOLZANO", "PROVINCIA AUTONOMA DI TRENTO", "VALLE D'AOSTA", "MOLISE", " PROVINCIA AUTONOMA DI BOLZANO"]
    
    list_regions = list_regions_without_first = list_regions_all[1:] # remove "CENTRALE"
    list_central = [list_regions_all[0]] if list_regions_all else [] # keep only "CENTRALE"

    print(f"List regions ({len(list_regions)}):", list_regions)
    print()

    print(f"List central ({len(list_central)}):", list_central)
    print()

    dic_thresholds = {"LAVORI": 5382000, "SERVIZI": 215000, "FORNITURE": 215000}
    for key, value in dic_thresholds.items():
        print("Key:", key)
        print("Value:", value)
        file_out_a = f"{Path(file_event_log_ted).stem}_{key}_above.csv"
        file_out_b = f"{Path(file_event_log_ted).stem}_{key}_below.csv"
        df_log_3_a, df_log_3_b = filter_df_by_region_and_amount(df_log, list_regions, dic_thresholds[key])
        df_log_3_a = df_log_3_a.sort_values(by=['case_id', 'event_timestamp'])
        df_log_3_b = df_log_3_b.sort_values(by=['case_id', 'event_timestamp'])
        print("Above shape:", df_log_3_a.shape)
        print("Below shape:", df_log_3_b.shape)
        path_log_a = Path(log_dir) / file_out_a
        path_log_b = Path(log_dir) / file_out_b
        print("Saving:", path_log_a)
        df_log_3_a.to_csv(path_log_a, sep=csv_sep, index=False, quoting=csv.QUOTE_MINIMAL)
        print("Saving:", path_log_b)
        df_log_3_b.to_csv(path_log_b, sep=csv_sep, index=False, quoting=csv.QUOTE_MINIMAL)
        print()

    # Stats about case duration
    print(">> Stats about case duration by oggetto_principale_contratto")
    path_stats = Path(log_dir) / "anac_log_2016_2022_duration_by_oggetto_contratto.csv"
    calculate_case_statistics(df_log, path_stats)
    
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
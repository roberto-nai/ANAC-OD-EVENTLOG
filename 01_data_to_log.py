# 01_data_to_log.py

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path
import csv

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import json_to_list_dict, json_to_sorted_dict, check_and_create_directory, list_files_by_type, get_values_from_dict_list, df_read_csv, df_print_details, distinct_values_frequencies, save_stats, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
od_anac_dir = str(yaml_config["OD_ANAC_DIR"])
od_file_type = str(yaml_config["OD_FILE_TYPE"])
csv_sep = str(yaml_config["CSV_FILE_SEP"])
tender_main_file = str(yaml_config["TENDER_MAIN_FILE"])
conf_file_cols_type = str(yaml_config["CONF_COLS_TYPE_FILE"]) 
conf_file_stats_inc = str(yaml_config["CONF_COLS_STATS_FILE"]) 
conf_file_filters = str(yaml_config["CONF_COLS_FILTER_FILE"]) 
conf_file_log = str(yaml_config["CONF_LOG_FILE"])             

stats_do = 0 # 0 if stats are not needed, else 1

stats_dir =  str(yaml_config["OD_STATS_DIR"])
log_dir =  str(yaml_config["EVENT_LOG_DIR"])

file_log_out = "anac_log_2016_2022.csv" # OUTPUT
file_log_caseids_out = "anac_log_2016_2022_caseids.csv" # OUTPUT: all the case-ids (CIG)

script_path, script_name = script_info(__file__)

### FUNCTIONS ###

def summarize_dataframe_to_dict(df: pd.DataFrame, file_name: str) -> dict:
    """
    Creates a dictionary summarizing the input DataFrame with the file name, and the count of missing (empty) values for each column.

    Parameters:
        df (pd.DataFrame): The DataFrame to summarize.
        file_name (str): The name of the file associated with the DataFrame.

    Returns:
        dict: a dictionary containing the file name and missing value counts for each column.
    """
    # Count the number of missing values in each column of the DataFrame
    missing_counts = df.isnull().sum()
    # Convert the Series to a dictionary
    missing_counts_dict = missing_counts.to_dict()
    # Count the number of duplicate rows, considering all columns
    duplicate_rows_count = df.duplicated().sum()
    # Get the number of rows and columns in the DataFrame
    num_rows, num_columns = df.shape
    # Calculate the ratio of duplicate rows to total rows
    ratio_dup = duplicate_rows_count / num_rows if num_rows > 0 else 0  # Avoid division by zero

    # Create the summary dictionary
    summary_dict = {
        'file_name': file_name,
        'rows_num':num_rows,
        'cols_num':num_columns,
        'missing_values': missing_counts_dict,
        'duplicated_rows': duplicate_rows_count,
        'duplicated_rows_perc': round(ratio_dup,2)
    }
    return summary_dict

def summarize_dataframe_to_df(summary_dict:dict) -> pd.DataFrame:
    """
    Saves the given summary dictionary to a CSV file, where each key-value pair in the dictionary becomes a column. The 'Missing Values Per Column' nested dictionary is expanded into separate columns.

    Parameters:
        summary_dict (dict): The summary dictionary to save.
        csv_file_name (str): The file name for the CSV file.

    Returns:
        pd.DataFrame: A dataframe with data.
    """
    # Flatten the 'Missing Values Per Column' dictionary into the main dictionary with prefix
    for key, value in summary_dict['missing_values'].items():
        summary_dict[f'Missing_{key}'] = value
    # Remove the original nested dictionary key
    del summary_dict['missing_values']
    
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([summary_dict])
    # Save the DataFrame to a CSV file
    # df.to_csv(csv_file_name, index=False)
    return df


    """
    Saves a DataFrame containing statistical data to both CSV and Excel file formats.

    Parameters:
        df_stats (pd.DataFrame): The DataFrame containing the statistics to be saved.
        file_name (str): The base name for the output files (without extension). The function will append '{stats_suffix}' to the base name.
        stats_suffix (str): The suffix of the stats type.
        csv_sep (str): The CSV separator.

    Returns:
        None
    """
    stats_out_csv = Path(stats_dir) / f"{file_name}{stats_suffix}.csv"
    print("Writing CSV:", stats_out_csv)
    df_stats.to_csv(stats_out_csv, sep=csv_sep, index=False)
    stats_out_xlsx = Path(stats_dir) / f"{file_name}{stats_suffix}.xlsx"
    xls_sheet_name=f"{file_name.removesuffix("_csv")[0:31]}" # For compatibility with older versions of Excel
    print("Writing XLSX:", stats_out_xlsx)
    print("XLSX sheet name:", xls_sheet_name)
    df_stats.to_excel(stats_out_xlsx, sheet_name=f"{xls_sheet_name}", index=False)

def create_event_log_dict(df: pd.DataFrame, mappings: list, event_name: str) -> pd.DataFrame:
    """
    Creates a dictionary suitable for use as an event log DataFrame with specified columns.
    
    Parameters:
        df (pd.DataFrame): The input DataFrame containing the event log data.
        mappings (list): A list of dictionaries where the keys are strings indicating the type of data ('event_log_data' or 'event_log_features') and the values are lists of column names.
        event_name (str): The constant name of the event to be added to each row.
    Returns:
        pd.DataFrame: A dictionary with the keys 'case_id', 'event_timestamp', and other specified features, or an error message if the required columns are not specified correctly.
    """
    case_id_col = None
    event_timestamp_col = None
    
    # Iterate over mappings to find 'case_id' and 'event_timestamp' columns
    for mapping in mappings:
        for key, columns in mapping.items():
            if 'event_log_data' in key and len(columns) == 2:
                case_id_col, event_timestamp_col = columns
    
    # Check if the necessary columns are specified
    if case_id_col is None or event_timestamp_col is None:
        return {"error": "Column names for case_id and event_timestamp are not specified correctly or are missing."}
    
    # Remove rows where event_timestamp_col is empty
    df = df.dropna(subset=[event_timestamp_col])

    # Initialize the result dictionary with case_id and event_timestamp
    result = {
        'case_id': df[case_id_col].tolist(),
        'event_name': [event_name] * len(df),
        'event_timestamp': df[event_timestamp_col].tolist()
    }
    
    # Add other event log features to the result dictionary
    for mapping in mappings:
        for key, columns in mapping.items():
            if 'event_log_features' in key:
                for column in columns:
                    if column not in result:
                        result[column] = df[column].tolist()
    
    return result

def fill_group_values(df, columns):
    for column in columns:
        df.loc[:, column] = df.groupby('case_id')[column].transform(lambda x: x.ffill().bfill())
    return df

### MAIN ###

def main():
    print()
    print(f"*** PROGRAM START ({script_name}) ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process: " + str(start_time))
    print()

    print(">> Preparing output directories")
    check_and_create_directory(stats_dir)
    check_and_create_directory(log_dir)
    print()

    print(">> Scanning Open Data catalogue")
    print("Directory:", od_anac_dir)
    list_od_files = list_files_by_type(od_anac_dir, od_file_type)
    list_od_files_len = len(list_od_files)
    print(f"Files '{od_file_type}' found: {list_od_files_len}")
    print()

    print(">> Reading the configuration file")
    
    print("File (columns type):", conf_file_cols_type)
    list_col_type_dic = json_to_sorted_dict(conf_file_cols_type)
    # print(list_col_type_dic) # debug
    
    print("File (stats columns):", conf_file_stats_inc)
    list_col_stats_dic = json_to_list_dict(conf_file_stats_inc)
    # print(list_col_stats_dic) # debug

    print("File (filter columns):", conf_file_filters)
    list_col_filters_dic = json_to_list_dict(conf_file_filters)
    # print(list_col_filters_dic) # debug

    print("File (event log columns):", conf_file_log)
    list_col_log_dic = json_to_list_dict(conf_file_log)
    # print(list_col_log_dic) # debug
    print()

    print(">> Reading Open Data files")
    
    list_log_df = []            # event log created for every dataframe
    list_log_df_mapping = []    # event log features for every dataframe
    list_cig = []               # IDs of tenders

    for file_od in list_od_files:
        # File info
        print("> Reading file")
        print("File:", file_od)
        file_path = Path(file_od)
        file_stem = file_path.stem # get the name without extension (is also the event name)

        # Get the columns to be included in stats by file name
        list_col_stats_inc = get_values_from_dict_list(list_col_stats_dic, file_od)
        list_col_stats_inc_len = len(list_col_stats_inc)

        # Get the columns to be filtered by file name
        list_col_filters = get_values_from_dict_list(list_col_filters_dic, file_od)
        list_col_filters_len = len(list_col_filters)

        # Read the file (dataset)
        list_col_exc = [] # no columns to exclude
        df_od = df_read_csv(od_anac_dir, file_od, list_col_exc, list_col_type_dic, None, csv_sep)
        df_print_details(df_od, f"File '{file_od}'")
        print()

        # For the main file tender_notice:
        # add the column "cpv_division" that takes the first two characters of "cod_cpv" if it's not null
        # add the column "accordo_quadro" (1 or 0)
        # from the column "settore" remove redundant "SETTORI "string
        if file_od == tender_main_file:
            print(f"> Updating main tender file '{file_od}'")
            df_od['cpv_division'] = df_od['cod_cpv'].apply(lambda x: x[:2] if pd.notnull(x) else None)
            df_od['accordo_quadro'] = df_od['cig_accordo_quadro'].apply(lambda x: "1" if pd.notna(x) else "0")
            df_od['accordo_quadro'] = df_od['accordo_quadro'].astype('object')
            df_od['settore'] = df_od['settore'].str.replace('SETTORI ', '')
            df_od['sezione_regionale'] = df_od['sezione_regionale'].str.replace('SEZIONE REGIONALE  ', '')
            df_od['sezione_regionale'] = df_od['sezione_regionale'].str.replace('SEZIONE REGIONALE ', '')
            df_od['sezione_regionale'] = df_od['sezione_regionale'].str.replace('PROVINCIA AUTONOMA DI', 'PA')
            df_od['oggetto_principale_contratto'] = df_od['oggetto_principale_contratto'].str.replace('FORNITURE', 'U') # sUpplies
            df_od['oggetto_principale_contratto'] = df_od['oggetto_principale_contratto'].str.replace('SERVIZI', 'S') # Services
            df_od['oggetto_principale_contratto'] = df_od['oggetto_principale_contratto'].str.replace('LAVORI', 'W') # Work
            df_print_details(df_od, f"File '{file_od}' (after cleaning)")

        if stats_do == 1:
            # Stats 1 - Missing values
            print(">> Creating stats")
            print("> Missing values")
            dic_od = summarize_dataframe_to_dict(df_od, file_od)
            # print(dic_od) # debug
            df_stats = summarize_dataframe_to_df(dic_od)
            # print(df_stats.head()) # debug
            print("> Saving stats")
            save_stats(df_stats, file_stem, "_stats_missing", stats_dir)
            print()

            # Stats 2 - Distinct values
            print("> Distinct values")
            print("Colums included for this stat:", list_col_stats_inc_len)
            print(list_col_stats_inc) # debug
            if list_col_stats_inc_len > 0:
                df_stats = distinct_values_frequencies(df_od, list_col_stats_inc)
                # print(df_stats.head()) # debug
                print("> Saving stats")
                save_stats(df_stats, file_stem, "_stats_distinct", stats_dir)
            print()

        # Filters
        if file_od == tender_main_file and list_col_filters_len > 0:
            print(">> Applying filters")
            print(f"Filters applied ({list_col_filters_len}):", list_col_filters)
            for filter_dict in list_col_filters:
                for key, value in filter_dict.items():
                    # print("Distinct values before filtering:", list(df_od[key].unique())) # debug
                    # print("Filter key:", key, "filter value:", value) # debug
                    df_od = df_od[df_od[key].isin(value)]
                    # print("DF size after filter:", df_od.shape) # debug
            df_print_details(df_od, f"File '{file_od}' (after filtering)")
            # Create list of ids (cig) to be kept in event log
            list_cig = list(df_od["cig"].unique())
            # list_cig
            print()

        # Create the log for this dataframe
        print("> Extracting event log data")
        # Get the columns to be filtered by file name
        list_col_log = get_values_from_dict_list(list_col_log_dic, file_od)
        list_col_log_len = len(list_col_log)
        print(f"Features for this dataframe ({list_col_log_len}): {list_col_log}")
        if list_col_log_len > 0:
            print("Event log for event:", file_stem)
            dic_log = create_event_log_dict(df_od, list_col_log, file_stem)
            if "error" not in dic_log:
                df_log = pd.DataFrame(dic_log)
                print("Event log shape:", df_log.shape)
                list_log_df.append(df_log)
                list_log_df_mapping.append(list_col_log)
        print("-"*3)
        print()

    print()

    # Final event log
    print(">> Merging the final event log")
    df_log_1 = pd.concat(list_log_df, ignore_index=True)
    
    df_log_1 = df_log_1[df_log_1['case_id'].isin(list_cig)] # Only keeps events whose case_id is also in the tender cig list 

    df_log_1['event_timestamp'] = pd.to_datetime(df_log_1['event_timestamp'])
    
    # Fix column types / nan
    # df_log_1['asta_elettronica'] = df_log_1['asta_elettronica'].fillna("0")
    # df_log_1['asta_elettronica'] = df_log_1['asta_elettronica'].replace({'0.0': '0', '1.0': '1'})
    # df_log_1['asta_elettronica'] = df_log_1['asta_elettronica'].astype(int)

    # Order
    df_log_2 = df_log_1.sort_values(by=['case_id', 'event_timestamp'])

    # Add case length
    df_log_2['case_len'] = df_log_2.groupby('case_id')['case_id'].transform('count')

    # Filter
    # Selection of the first event for each case_id
    first_events = df_log_2.groupby('case_id').first().reset_index()

    # Filtering of case_ids whose first event is 'TENDER_NOTICE'
    valid_case_ids = first_events[first_events['event_name'] == 'TENDER_NOTICE']['case_id']

    # Filtering the original DataFrame to keep only valid case_ids
    df_log_3 = df_log_2[df_log_2['case_id'].isin(valid_case_ids)]

    # Add trace attributes to all the rows
    columns_to_fill = ["oggetto_principale_contratto", "importo_lotto", "accordo_quadro", "cpv_division", "sezione_regionale", "cod_tipo_scelta_contraente", "cod_modalita_realizzazione"]
    # df_log_3 = df_log_3.groupby('case_id').apply(lambda group: fill_group_values(group, columns_to_fill))
    df_log_3 = fill_group_values(df_log_3, columns_to_fill)

    # Removes 'UNCLASSIFIED' regions
    list_region_remove = ["NON CLASSIFICATO"]
    df_log_3 = df_log_3[~df_log_3['sezione_regionale'].isin(list_region_remove)]

    # Print
    df_print_details(df_log_3, f"Event log")

    # Save the event log
    path_log = Path(log_dir) / file_log_out
    print("Saving final event log to:", path_log)
    df_log_3.to_csv(path_log, sep=csv_sep, index=False, quoting=csv.QUOTE_MINIMAL)
    print()

    # Save the list of CIG (case-id) of the event log (to be searche in TED texts)
    df_log_3_cig = df_log_3[["case_id"]]
    df_print_details(df_log_3_cig, f"Case IDs")
    path_log = Path(log_dir) / file_log_caseids_out
    print("Saving final event log Case IDs to:", path_log)
    df_log_3_cig.to_csv(path_log, sep=csv_sep, index=False, quoting=csv.QUOTE_MINIMAL)
    print()

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
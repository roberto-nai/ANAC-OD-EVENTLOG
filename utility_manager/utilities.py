import json
from pathlib import Path
import pandas as pd 

def json_to_list_dict(json_file: str) -> list:
    """
    Extracts and sorts key-value pairs from a JSON file alphabetically by the keys.

    Parameters:
        json_file (str): The path to the JSON file.

    Returns:
        list: A list of tuples containing tuples of key-value pairs extracted and sorted from the JSON file.
    """
    # Load JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Extract key-value pairs into a list of dictionaries and sort alphabetically by keys
    sorted_key_value_pairs = [{key: value} for key, value in sorted(data.items())]
    
    return sorted_key_value_pairs

def json_to_sorted_dict(json_file: str) -> dict:
    """
    Extracts and sorts key-value pairs from a JSON file alphabetically by the keys.

    Parameters:
        json_file (str): The path to the JSON file.

    Returns:
        dict: A dictionary containing key-value pairs extracted and sorted from the JSON file.
    """
    # Load JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Sort key-value pairs alphabetically by keys and return as a single dictionary
    sorted_key_value_pairs = {key: data[key] for key in sorted(data.keys())}
    
    return sorted_key_value_pairs

def check_and_create_directory(dir_name:str, dir_parent:str="") -> None:
    """
    Create a directory in its parent directory (optional).

    Parameters:
        dir_name (str): directory to be created.
        dir_parent (str): parent directory in which to create the directory.
    """

    path_directory = ""
    if dir_parent != "":
        path_directory = Path(dir_parent) / dir_name
    else:
        path_directory = Path(dir_name)
    if path_directory.exists() and path_directory.is_dir():
        print("The directory '{}' already exists: {}".format(dir_name, path_directory))
    else:
        path_directory.mkdir(parents=True, exist_ok=True)
        print("The directory '{}' has been created successfully: {}".format(dir_name, path_directory))


def list_files_by_type(directory:str, extension:str) -> list:
    """
    List files in the given directory with a specific extension, excluding macOS temporary files.
    
    Parameters:
        directory (str): The directory path to search in.
        extension (str): The file extension to filter by, including the leading dot (e.g., '.txt').

    Returns:
        A list of file names matching the extension and not being macOS temporary files.
    """
    # Create a Path object for the directory
    dir_path = Path(directory)
    file_list = []
    # List all files with the specified extension and filter out macOS temporary files
    file_list = [file.name for file in dir_path.glob(f'*{extension}') if not file.name.startswith('._')]
    return file_list


def get_values_from_dict_list(dict_list: list, key: str) -> list:
    """
    Given a list of dictionaries and a key, this function returns the list of values associated with the key.
    If the key is not found in any dictionary, an empty list is returned.

    Parameters:
        dict_list (list): List of dictionaries where each dictionary has a string key and a list of strings as values.
        key (str): Key to search for in the dictionaries.
    
    Returns
        List of values associated with the key or an empty list if the key is not found.
    """
    # Iterate over each dictionary in the list
    for dictionary in dict_list:
        # Check if the key exists in the dictionary
        if key in dictionary:
            # Return the value associated with the key
            return dictionary[key]
    # Return an empty list if the key is not found
    return []


def df_read_csv(dir_name: str, file_name: str, list_col_exc: list, list_col_type:dict, nrows:int, csv_sep: str = ";") -> pd.DataFrame:
    """
    Reads data from a CSV file into a pandas DataFrame excluding columns (if needed)

    Parameters:
        dir_name (str): the directory to the CSV file to be read.
        file_name (str): the filename to the CSV file to be read.
        list_col_exc (list): columns to be excluded.
        list_col_type (dict): columns type.
        nrows (int): rows to be read (if None, all).
        sep (str, optional): the delimiter string used in the CSV file. Defaults to ';'.

    Returns:
        pd.DataFrame: a pandas DataFrame containing the data read from the CSV file.
    """
    path_data = Path(dir_name) / file_name
    if nrows is not None:
        df = pd.read_csv(path_data, sep=csv_sep, dtype=list_col_type, nrows=nrows, low_memory=False)
    else:
        df = pd.read_csv(path_data, sep=csv_sep, dtype=list_col_type, low_memory=False)
    if len(list_col_exc) > 0:
        for col_name in list_col_exc:
                if col_name in df.columns:
                    del df[col_name]
    df = df.drop_duplicates()
    return df


def df_print_details(df: pd.DataFrame, title: str) -> None:
    """
    Prints details of a pandas DataFrame, including its size and a preview of its contents.

    Parameters:
        df (pd.DataFrame): the DataFrame whose details are to be printed.
        title (str): a title for the printed output to describe the context of the DataFrame.

    Returns:
        None
    """

    #print(f"{title}")
    print(f"Dataframe size: {df.shape}")
    # print(df.dtypes, "\n") # debug
    print(f"{title} - dataframe preview:")
    print(df.head(), "\n\n")
    print(df.columns, "\n")
    print(df.shape, "\n")

def script_info(file: str) -> tuple:
    """
    Returns the absolute path and the base name of the script file provided.

    Parameters:
        file (str): The file path to the script.

    Returns:
        tuple: A tuple containing the absolute path and the base name of the script.
    """
    
    script_path = Path(file).resolve()  # Converts the path to an absolute path
    script_name = script_path.name      # Gets the file name including extension

    return script_path, script_name


def distinct_values_frequencies(df: pd.DataFrame, include_cols: list) -> pd.DataFrame:
    """
    Extracts the distinct values and their frequencies in percentage for each column of the given dataframe, excluding specified columns.
    
    Parameters:
        df (pd.DataFrame): The input dataframe.
        include_cols (list): A list of column names to be included int the analysis.
    
    Returns:
        pd.DataFrame: A dataframe containing the distinct values and their frequencies  in percentage for each column of the input dataframe, excluding  the specified columns.
    """
    # Use only the specified columns
    df_filtered = df[include_cols]
    # df_filtered = df.loc[:, include_cols]
    
    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame(columns=['Column', 'Value', 'Frequency (%)'])
    
    # List to store the results
    result_list = []

    # Calculate the distinct values and their frequencies
    for col in df_filtered.columns:
        value_counts = df_filtered[col].value_counts(normalize=True) * 100
        value_counts = value_counts.round(2)  # Round frequencies to two decimal places
        for value, freq in value_counts.items():
            # result_df = pd.concat([result_df, pd.DataFrame({'Column': [col], 'Value': [value], 'Frequency (%)': [freq]})], ignore_index=True)
            result_list.append({'Column': col, 'Value': value, 'Frequency (%)': freq})
    
    # Create the result DataFrame from the list
    result_df = pd.DataFrame(result_list, columns=['Column', 'Value', 'Frequency (%)'])
    
    return result_df

def save_stats(df_stats:pd.DataFrame, file_name:str, stats_suffix:str, stats_dir:str, csv_sep:str = ";") -> None:
    """
    Saves a DataFrame containing statistical data to both CSV and Excel file formats.

    Parameters:
        df_stats (pd.DataFrame): The DataFrame containing the statistics to be saved.
        file_name (str): The base name for the output files (without extension). The function will append '{stats_suffix}' to the base name.
        stats_suffix (str): The suffix of the stats type.
        stats_dir (str): The directory in which save the stats.
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
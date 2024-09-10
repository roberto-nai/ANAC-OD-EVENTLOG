# ANAC Open Data to Event Log

![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)

### > Directories

#### config
Configuration directory with ```config.yml```.  

#### open_data_anac
Directory with downloaded ANAC Open Data Catalogue (see this project: [https://github.com/roberto-nai/ANAC-OD-DOWNLOADER](https://github.com/roberto-nai/ANAC-OD-DOWNLOADER)).  
Open Data are also available on Zenodo: [https://doi.org/10.5281/zenodo.11452793](https://doi.org/10.5281/zenodo.11452793).  

#### stats
Directory with procurements stats.

#### utility_manager
Directory with utilities functions.

### > Script Execution

#### ```01_data_to_log.py```
Loads the various datasets (in CSV format) and generates the event log. Only keeps cases starting with the TENDER_NOTICE event.  

#### ```02_log_filter_TED.py```
Filters the event log keeping only the case-ids (CIG) present in TED texts.  

#### ```03_log_filter_threshold.py```
Divides the event log by type (Works, Supplies, Services) and amount (above/below threshold).  

### > Configurations

#### ```conf_cols_filter.json```
List of columns (features) to be filtered.  

#### ```conf_cols_filter.json```
List of columns (features) to be filtered.  

#### ```conf_cols_filter.json```
List of columns (features) to be filtered.  

#### ```conf_cols_filter.json```
List of columns (features) to be filtered.  

### > Script Dependencies
See ```requirements.txt``` for the required libraries (```pip install -r requirements.txt```).  

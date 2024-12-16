##########################################

# This python script will retrieve datasets from a specified folder of raw data and will merge them based on specified variable names

##########################################


### LIBRARIES
# pip install xlrd
import xlrd # for .xls
import pandas as pd
import numpy as np
from pathlib import Path
import os
from extract_supporting_facilities_main import validate_id_input # validating IDs functionality
import re # for file reading and text extraction 


### FUNCTIONS
def read_dataset(file_path, filename):
    """Read dataset based on file extension and extract year and quarter from filename."""
    ext = file_path.suffix.lower()
    df = None
    year, quarter = extract_date_info(filename)
    # Attempt to read file based on extension
    try:
        if ext == '.csv':
            df = pd.read_csv(file_path)
        elif ext in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
    except Exception as e:
        print(f"Error reading file: {e}")
        return None, None, None
    # Extract year and quarter
    df['year_var'] = year
    df['quarter_var'] = quarter
    # Reordering columns so year_var and quarter_var come first
    new_column_order = ['year_var', 'quarter_var'] + [col for col in df.columns if col not in ('year_var', 'quarter_var')]
    df = df[new_column_order]
    return df


def extract_date_info(filename):
    """Extract year and quarter"""
    year = next((y for y in re.findall(r'(?:19[5-9]\d|20[0-2]\d)', filename)), '.')
    quarter_match = re.search(r'Quarter[_\s](\d)|Q(\d)', filename, re.IGNORECASE)
    quarter = f"Q{quarter_match.group(1) or quarter_match.group(2)}" if quarter_match else '.'
    return year, quarter


def filter_columns(df, action, columns):
    """Filter dataframe columns based on action"""
    if action == 'keep':
        return df[columns]
    else:  # drop
        return df.drop(columns=columns)

def filter_rows(df, start_row):
    """Filter dataframe rows starting from specified row"""
    try:
        # Define missing value indicators
        missing_values = ['', ' ', '.', '-', 'nan', 'NaN', 'NAN', 'na', 'Na', 'NA', 
                         '/', '\\', 'null', 'NULL', 'none', 'None', 'NONE']
        # Replacing any possible missing value with stirng 'NA' so that it is readable column when dropping rows
        df = df.replace(missing_values, 'NA')
        df = df.fillna('NA')
        start_idx = int(start_row)
        return df.iloc[start_idx:].reset_index(drop=True)
    except ValueError:
        print("Invalid row number, keeping original dataset")
        return df

def rename_selected_columns(df):
    """Rename specific columns in dataframe"""
    while True:
        print("\nCurrent columns:", df.columns.tolist())
        rename = input("Do you want to rename a column? (yes/no): ").lower()
        if rename != 'yes':
            break
            
        col_name = input("Which column do you want to rename?: ")
        if col_name not in df.columns:
            print("Column not found")
            continue
            
        new_name = input("Enter new column name: ")
        df = df.rename(columns={col_name: new_name})
    return df

def append_datasets(datasets):
    """Append datasets vertically"""
    if len(datasets) < 2:
        print("Need at least 2 datasets to append")
        return None
        
    print("\nAvailable datasets:")
    for i, (name, df) in enumerate(datasets.items(), 1):
        print(f"{i}. {name}")
        print(f"   Columns: {df.columns.tolist()}\n")
    
    try:
        appended_df = pd.concat(datasets.values(), axis=0, ignore_index=True)
        appended_df = rename_selected_columns(appended_df)
        print(f"\nAppended dataset shape: {appended_df.shape}")
        return appended_df
    except Exception as e:
        print(f"Error during append: {e}")
        return None    
    
    
    

### MAIN EXECUTION
def main():
    # Defining directories
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent
    except NameError:
        BASE_DIR = Path.cwd()
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "supporting-facilities")

    if not os.path.exists(RAW_DATA_DIR):
        print(f"Directory {RAW_DATA_DIR} does not exist.")
        return
        
    files = sorted(os.listdir(RAW_DATA_DIR)) # in sorted order
    
    if not files:
        print(f"No files found in {RAW_DATA_DIR}")
        return
        
    print(f"\nFiles in {RAW_DATA_DIR}:")
    print("-" * 50)
    for i, file in enumerate(sorted(files), 1):
        file_path = os.path.join(RAW_DATA_DIR, file)
        file_size = os.path.getsize(file_path)
        print(f"{i}. {file} ({file_size/1024:.1f} KB)")
    
    
    # User input for IDs
    while True:            
        selected_files = input("Which files do you wish to explore? Enter individual IDs before listed filename (e.g., 1,3,5), range of IDs(e.g., 4-7), combination of specific IDs and range (e.g. 1-3, 8) or all listed files by typing 'all': ")
        selected_ids = validate_id_input(selected_files, len(files))
        if selected_ids is not None:
            break
        print("Please try again.\n")
            
    # Output
    datasets = {}
    for id in selected_ids:
        file = files[id - 1]
        file_path = Path(RAW_DATA_DIR) / file
        print(f"\nReading {file} ...")
        
        df = read_dataset(file_path, file)

        if df is None:
            continue
            
        print("\nDataset Info:")
        print(df.info())
        print("\nShape:", df.shape)
        print("\nColumns:", df.columns.tolist())
        print("\nFirst few rows:")
        print(df.head(30))
        
        filter_input = input("\nNeed to drop first few rows? (yes/no): ").lower()
        if filter_input == 'yes':
            filter_input_2 = input("\nWhich row do you want to start at?: ").lower()
            df = filter_rows(df, filter_input_2)
            
            # Using first row values as column names
            try:
                # Store original names of the first two columns
                year_column_name = df.columns[0]
                quarter_column_name = df.columns[1]
                # Creating a new list of column names
                new_columns = [year_column_name, quarter_column_name] + list(df.iloc[0, 2:])
                # Apply new column names to the DataFrame
                df.columns = new_columns
                df = df.iloc[1:].reset_index(drop=True)
            except Exception as e:
                print(f"Error setting column names: {e}")

            print("\nModified dataset info:")
            print(df.info())
            print("\nShape:", df.shape)
            print("\nColumns:", df.columns.tolist())
            print("\nFirst few rows:")
            print(df.head(10))
            
            filter_option_input = input("Use 'keep' or 'drop' for variable filtration, or 'neither'?: ").lower()
            if filter_option_input in ['keep', 'drop']:
                print("\nAvailable columns:", df.columns.tolist())
                cols = input("Enter column names (comma-separated): ").split(',')
                cols = [c.strip() for c in cols]
                
                if all(col in df.columns for col in cols):
                    df = filter_columns(df, filter_option_input, cols)
                    print(f"Dataset modified with {filter_option_input} columns")
                else:
                    print("Invalid column name(s), keeping original dataset")

            # Column renaming option
            rename_option = input("\nDo you want to rename columns? (yes/no): ").lower()
            if rename_option == 'yes':
                df = rename_selected_columns(df)
            
            # Save option
            save_input = input("\nSave to dataframe dictionary? (yes/no): ").lower()
            if save_input == 'yes':
                datasets[file] = df
    
    print(f"\nStored {len(datasets)} datasets in dictionary")
    
    append_input = input("Do you want to merge the datasets (yes/no)?: ").lower()
    if append_input == 'yes':
        append_data = append_datasets(datasets)
        if append_data is not None:
            return {'appended': append_data , **datasets}
    return datasets

if __name__ == "__main__":
    datasets = main()
    
    
    
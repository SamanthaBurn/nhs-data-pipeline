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
    """
    Read dataset based on file extension and extract year and quarter from filename.
    For this data, we assume no .csv files, only .xls and .xlsx
    """
    try:
        year, quarter_info, is_all_quarters = extract_date_info(filename)
        
        if is_all_quarters:
            # Read all sheets and combine them
            dfs = []
            excel_file = pd.ExcelFile(file_path)
            
            for sheet in excel_file.sheet_names:
                for month_pattern, quarter in quarter_info.items():
                    if month_pattern in sheet:
                        df_sheet = pd.read_excel(file_path, sheet_name=sheet)
                        df_sheet['year_var'] = year
                        df_sheet['quarter_var'] = quarter
                        dfs.append(df_sheet)
            
            if dfs:
                df = pd.concat(dfs, ignore_index=True)
            else:
                df = pd.read_excel(file_path)  # Fallback to reading entire file
                df['year_var'] = year
                df['quarter_var'] = '.'
        else:
            # Regular file reading
            df = pd.read_excel(file_path)  # Removed csv option as per comment
            df['year_var'] = year
            df['quarter_var'] = quarter_info  # quarter_info is just quarter in this case    
        
        # Reordering columns so year_var and quarter_var come first
        new_column_order = ['year_var', 'quarter_var'] + [col for col in df.columns if col not in ('year_var', 'quarter_var')]
        df = df[new_column_order]
        return df
        
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def extract_date_info(filename):
    """
    Extract year and quarter from filename or sheet names
    """
    # Extract year
    year = next((y for y in re.findall(r'(?:19[5-9]\d|20[0-2]\d)', filename)), '.')
    
    # Data before 2009-10
    if 'All_quarters' in filename:
        # Convert full year to YY format for sheet matching
        year_suffix = year[-2:] if year != '.' else ''
        year_suffix_int = int(year_suffix)
        year_suffix_plus_one = year_suffix_int + 1
        if 0 <= year_suffix_int < 10: # one-digit year
            year_suffix_plus_one_str = f"0{year_suffix_plus_one}"
        else:
            year_suffix_plus_one_str = str(year_suffix_plus_one)
        # Dictionary mapping month patterns to quarters
        quarter_map = {
            f'June{year_suffix}': 'Q1',
            f'Sep{year_suffix}': 'Q2',
            f'Dec{year_suffix}': 'Q3',
            f'Mar{year_suffix_plus_one_str}': 'Q4'
        }
        return year, quarter_map, True
    
    # Data from 2009-10 and after
    else:
        quarter_match = re.search(r'Quarter[_\s](\d)|Q(\d)', filename, re.IGNORECASE)
        quarter = f"Q{quarter_match.group(1) or quarter_match.group(2)}" if quarter_match else '.'
        return year, quarter, False

def filter_rows(df, variable_name_str):
    """
    Filter dataframe rows starting from variable name.
    """
    try:
        # Define missing value indicators
        missing_values = ['', ' ', '.', '-', 'nan', 'NaN', 'NAN', 'na', 'Na', 'NA', 
                         '/', '\\', 'null', 'NULL', 'none', 'None', 'NONE']
        # Replace missing values
        df = df.replace(missing_values, 'NA')
        df = df.fillna('NA')
        # Find the row containing "Number of operating theatres"
        target_row = None
        for idx, row in df.iterrows():
            if row.astype(str).str.contains(variable_name_str, case=False).any():
                target_row = idx
                break
        if target_row is not None:
            filtered_df = df.iloc[target_row:].reset_index(drop=True)
            return filtered_df
        else:
            print(f"{variable_name_str} not found in dataset, keeping original dataset")
            return df
    except Exception as e:
        print(f"Error during filtering: {e}, keeping original dataset")
        return df

def rename_selected_columns(df):
    """
    Rename specific columns in dataframe
    """
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


def append_datasets(datasets, year_var_str, quarter_var_str):
    """
    Append datasets vertically.
    """
    if len(datasets) < 2:
        print("Need at least 2 datasets to append")
        return None
        
    print("\nAvailable datasets:")
    for i, (name, df) in enumerate(datasets.items(), 1):
        print(f"{i}. {name}")
        print(f"   Columns: {df.columns.tolist()}\n")
    
    try:
        appended_df = pd.concat(datasets.values(), axis=0, ignore_index=True)
        appended_df = appended_df.sort_values(by=[year_var_str, quarter_var_str], ascending=True)
        appended_df = appended_df.reset_index(drop=True)
        appended_df = rename_selected_columns(appended_df)
        print(f"\nAppended dataset shape: {appended_df.shape}")
        return appended_df
    except Exception as e:
        print(f"Error during append: {e}")
        return None    
    
def consolidate_columns(df, column_groups):
    """
    Efficiently consolidate multiple column groups into single columns.
    Used in data cleaning.
    """
    try:
        # Create all new columns at once
        for new_col, old_cols in column_groups.items():
            # Convert to string type to avoid type mismatches
            cols_to_combine = [df[col].astype(str) for col in old_cols if col in df.columns]
            if cols_to_combine:
                # Replace 'nan' and 'NA' with None for proper coalescing
                for i, col in enumerate(cols_to_combine):
                    cols_to_combine[i] = col.replace({'nan': None, 'NA': None})
                
                # Combine all columns using reduce
                from functools import reduce
                df[new_col] = reduce(lambda x, y: x.combine_first(y), cols_to_combine)
        
        # Reorder columns to put new columns after quarter_var
        old_cols = df.columns.tolist()
        new_cols = [col for col in old_cols if col not in column_groups.keys()]
        insert_pos = new_cols.index('quarter_var') + 1
        for new_col in column_groups.keys():
            new_cols.insert(insert_pos, new_col)
            insert_pos += 1
            
        return df[new_cols]
        
    except Exception as e:
        print(f"Error consolidating columns: {e}")
        return df
    

### MAIN EXECUTION
def main():
    # Defining directories
    try:
        BASE_DIR = Path(__file__).resolve().parent.parent
    except NameError:
        BASE_DIR = Path.cwd()
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "rawdata" / "supporting-facilities")
    DATA_DIR = os.getenv("RAW_DATA_DIR", BASE_DIR / "data")

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
            
#        print("\nDataset Info:")
#        print(df.info())
#        print("\nShape:", df.shape)
#        print("\nColumns:", df.columns.tolist())
#        print("\nFirst few rows:")
#        print(df.head(30))
        
        # Filtering by variable name: "Of which, number of dedicated day case theatres"
        df = filter_rows(df, 'Of which, number of dedicated day case theatres')
        
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
        
        datasets[file] = df
    
    print(f"\nStored {len(datasets)} datasets in dictionary")
    
    append_input = input("Do you want to merge the datasets (yes/no)?:\n(Dataset will be sorted by year_var and quarter_var)").lower()
    if append_input == 'yes':
        append_data = append_datasets(datasets, 'year_var', 'quarter_var')
        if append_data is not None:
            save_data_input = input("Do you want to save this dataset as .csv in local directory (yes/no)?: ").lower()
            if save_data_input == 'yes':
                try:
                    output_path = os.path.join(DATA_DIR, 'supporting-facilities.csv')
                    append_data.to_csv(output_path, index=False)
                    print(f"Dataset successfully saved to {output_path}")
                except Exception as e:
                    print(f"Error saving dataset: {e}")
            return {'appended': append_data, **datasets}
    return datasets
    
    return datasets

if __name__ == "__main__":
    datasets = main()    
    
    
    
    
    
### CLEANING

# Checking if all years have quarters
    
df = datasets['appended']

for year in df['year_var'].unique():
    df_filtered = df[df['year_var'] == year]
    print(f"Quarters for {year}")
    print(df_filtered['quarter_var'].unique())
    
print(df.info())

# Creating single vars for measure:
# - organisation code
# - number of operating theatres
# - number of daycase theatres


# Defining the new columns we want to create
column_mappings = {
    'SHA_2': ['SHA', 'SHA Code'],
    'organisation_code': ['OrgID', 'Organisation Code'],
    'organisation_name': ['Name', 'Organisation Name'],
    'area_team_code': ['Area Team Code'],
    'area_team_name': ['Area Team Name'],
    'region_code': ['Region Code'],
    'region_name': ['Region Name'],
}

df = consolidate_columns(df, column_mappings)

df = df.drop(columns = ['SHA', 'OrgID', 'Name',
                        'NA', 'SHA Code',
                        'Organisation Code', 'Organisation Name', 
                        'Area Team Code', 'Area Team Name', 
                        'Region Code', 'Region Name']) # unnecessary column

df.rename(columns={'SHA_2': 'SHA',
                   'Number of operating theatres': 'nr_operating_theatres',
                   'Of which, number of dedicated day case theatres': 'nr_day_case_theatres'}, inplace=True)



# Getting final data and cleaning for unimportant rows from merging different raw datasets (e.g. "Source")
datasets_2 = []  # Use list instead of dict since we're appending

# Filtering the data for each year then dropping rows
for year in df['year_var'].unique():
    df_filtered = df[df['year_var'] == year].copy()  # Use copy to avoid SettingWithCopyWarning
    df_filtered = df_filtered.dropna(subset=['organisation_code'])
    mask_theatres = (df_filtered['nr_day_case_theatres'] != 'NA') & \
                    (df_filtered['nr_day_case_theatres'] != 'Of which, number of dedicated day case theatres')
    mask_org = (df_filtered['organisation_name'] != 'England (Including Independent Sector)') & \
                (df_filtered['organisation_name'] != 'England (Excluding Independent Sector)')
    df_filtered = df_filtered[mask_theatres & mask_org]
    if not df_filtered.empty:
        datasets_2.append(df_filtered)
        print(f"Added dataset for year {year} with {len(df_filtered)} rows")
# Merge all datasets after the loop
if datasets_2:
    final_df = pd.concat(datasets_2, axis=0, ignore_index=True)
    final_df = final_df.sort_values(by=['year_var', 'quarter_var'], ascending=True)
    print(f"\nFinal dataset shape: {final_df.shape}")
else:
    print("No datasets to merge")

# Checking for quarters across years
for year in final_df['year_var'].unique():
    df_filtered = final_df[final_df['year_var'] == year]
    print(f"Quarters for {year}")
    print(df_filtered['quarter_var'].unique())


# Saving
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
output_path = os.path.join(DATA_DIR, 'supporting-facilities_clean.csv')
final_df.to_csv(output_path, index=False)






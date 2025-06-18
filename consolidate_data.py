import os
import pandas as pd
import logging

# --- Configuration ---
# Directory where raw scraped data is stored
RAW_DATA_DIR = "data/raw"
# Directory where processed (consolidated) data will be saved
PROCESSED_DATA_DIR = "data/processed"
# Name of the final consolidated CSV file
CONSOLIDATED_FILE_NAME = "consolidated_soil_nutrient_data.csv"
# Log file to record consolidation progress and errors
LOG_FILE = "consolidation_log.log"

# --- Logging Setup ---
# Configure logging to write messages to both a file and the console.
# INFO level messages and above will be recorded.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),  # Log to file
                        logging.StreamHandler()         # Log to console
                    ])

# --- Main Consolidation Logic ---
def consolidate_data():
    """
    Reads raw MacroNutrient and MicroNutrient CSV files, merges them,
    handles missing values, standardizes column names and data types,
    and saves the consolidated data to a single CSV file.
    """
    all_data_frames = [] # List to hold DataFrames from all blocks
    logging.info(f"Starting data consolidation process from {RAW_DATA_DIR}")

    # Iterate through the hierarchical raw data directory structure: Year -> State -> District
    for year_dir in os.listdir(RAW_DATA_DIR):
        year_path = os.path.join(RAW_DATA_DIR, year_dir)
        if not os.path.isdir(year_path):
            continue # Skip if it's not a directory

        logging.info(f"Processing year: {year_dir}")
        for state_dir in os.listdir(year_path):
            state_path = os.path.join(year_path, state_dir)
            if not os.path.isdir(state_path):
                continue # Skip if it's not a directory

            # Attempt to revert the safe name back to original, for logging and metadata
            # This is a basic un-sanitization; for robust applications, a mapping might be needed.
            state_name = state_dir.replace("_", " ")

            logging.info(f"  Processing state: {state_name}")
            for district_dir in os.listdir(state_path):
                district_path = os.path.join(state_path, district_dir)
                if not os.path.isdir(district_path):
                    continue # Skip if it's not a directory

                # Attempt to revert the safe name back to original
                district_name = district_dir.replace("_", " ")
                logging.info(f"    Processing district: {district_name}")

                macro_file = None
                micro_file = None
                current_block_name = "" # Initialize block name for this iteration

                # Find the MacroNutrient and MicroNutrient CSV files within the current district directory
                for file_name in os.listdir(district_path):
                    if file_name.endswith("_macro.csv"):
                        macro_file = os.path.join(district_path, file_name)
                        # Extract block name from the file name
                        current_block_name = file_name.replace("_macro.csv", "").replace("_", " ")
                    elif file_name.endswith("_micro.csv"):
                        micro_file = os.path.join(district_path, file_name)
                        # Extract block name from the file name (assuming it's the same block)
                        current_block_name = file_name.replace("_micro.csv", "").replace("_", " ")

                if not macro_file and not micro_file:
                    logging.warning(f"      No macro or micro files found in {district_path}. Skipping this district's block.")
                    continue # Skip to the next district if no data files are found

                df_macro = pd.DataFrame()
                df_micro = pd.DataFrame()

                # Load MacroNutrient data
                if macro_file:
                    try:
                        df_macro = pd.read_csv(macro_file)
                        logging.info(f"        Loaded MacroNutrient data from {os.path.basename(macro_file)}")
                    except Exception as e:
                        logging.error(f"        Error loading MacroNutrient file {macro_file}: {e}")
                        df_macro = pd.DataFrame() # Set to empty DataFrame on error

                # Load MicroNutrient data
                if micro_file:
                    try:
                        df_micro = pd.read_csv(micro_file)
                        logging.info(f"        Loaded MicroNutrient data from {os.path.basename(micro_file)}")
                    except Exception as e:
                        logging.error(f"        Error loading MicroNutrient file {micro_file}: {e}")
                        df_micro = pd.DataFrame() # Set to empty DataFrame on error

                # Consolidate dataframes based on availability
                if not df_macro.empty and not df_micro.empty:
                    # Identify common column for merging (e.g., 'Village', 'Gram Panchayath')
                    # This step is crucial as the column name might vary.
                    common_col_macro = [col for col in df_macro.columns if 'village' in col.lower() or 'gram' in col.lower()]
                    common_col_micro = [col for col in df_micro.columns if 'village' in col.lower() or 'gram' in col.lower()]

                    if common_col_macro and common_col_micro:
                        # Rename the common column to a consistent name for merging
                        df_macro.rename(columns={common_col_macro[0]: 'Village_GramPanchayath'}, inplace=True)
                        df_micro.rename(columns={common_col_micro[0]: 'Village_GramPanchayath'}, inplace=True)

                        try:
                            # Perform an outer merge to keep all records from both tables
                            # Suffixes are added to overlapping column names (other than the merge key)
                            merged_df = pd.merge(df_macro, df_micro, on='Village_GramPanchayath', how='outer', suffixes=('_macro', '_micro'))
                            logging.info(f"        Merged Macro and Micro data for Block: {current_block_name}")
                        except Exception as e:
                            logging.error(f"        Error merging Macro and Micro data for Block {current_block_name}: {e}. Falling back to concatenation.")
                            # Fallback if merging fails unexpectedly (e.g., non-unique merge key issues)
                            merged_df = pd.concat([df_macro, df_micro], axis=1)
                    else:
                        logging.warning(f"        Could not find common village/gram panchayath column for merging block {current_block_name}. Concatenating instead.")
                        # If no common column found, simply concatenate side-by-side
                        merged_df = pd.concat([df_macro, df_micro], axis=1)

                elif not df_macro.empty:
                    merged_df = df_macro
                    logging.warning(f"        Only MacroNutrient data found for Block: {current_block_name}.")
                elif not df_micro.empty:
                    merged_df = df_micro
                    logging.warning(f"        Only MicroNutrient data found for Block: {current_block_name}.")
                else:
                    merged_df = pd.DataFrame() # No data loaded for this block

                if not merged_df.empty:
                    # Add geographical and year identifiers to the merged DataFrame
                    merged_df['Year'] = year_dir
                    merged_df['State'] = state_name
                    merged_df['District'] = district_name
                    merged_df['Block'] = current_block_name.strip() # Ensure no leading/trailing spaces

                    all_data_frames.append(merged_df) # Add to the list for final concatenation

    if not all_data_frames:
        logging.warning("No data frames were collected for consolidation. Please ensure 'get_raw_data.py' was run successfully.")
        return # Exit if no data was collected

    # Concatenate all collected DataFrames into one master DataFrame
    final_df = pd.concat(all_data_frames, ignore_index=True)
    logging.info(f"Successfully collected and concatenated {len(all_data_frames)} data frames into a single DataFrame.")
    logging.info(f"Total rows in consolidated data: {len(final_df)}")

    # --- Data Cleaning and Transformation ---

    # 1. Handle Missing Values: Fill NaNs based on column type
    logging.info("Handling missing values...")
    for col in final_df.columns:
        if final_df[col].dtype in ['float64', 'int64']:
            # Fill numerical NaNs with 0 (a common approach for nutrient data, adjust if needed)
            final_df[col] = final_df[col].fillna(0)
        else:
            # Fill categorical/object NaNs with 'Unknown'
            final_df[col] = final_df[col].fillna('Unknown')
    logging.info("Missing values handled.")

    # 2. Standardize Column Names: Convert to snake_case, remove special characters
    logging.info("Standardizing column names...")
    new_columns = {}
    for col in final_df.columns:
        # Convert to lowercase, replace spaces with underscores
        new_col = col.strip().lower().replace(' ', '_').replace('-', '_')
        # Replace common special characters found in column names
        new_col = new_col.replace('.', '').replace('%', 'percent').replace('/', '_per_')
        # Remove any remaining non-alphanumeric characters except underscores
        new_col = ''.join(e for e in new_col if e.isalnum() or e == '_')
        # Handle duplicate standardized names if any
        if new_col in new_columns.values():
            count = 1
            temp_new_col = new_col
            while temp_new_col in new_columns.values():
                temp_new_col = f"{new_col}_{count}"
                count += 1
            new_col = temp_new_col
        new_columns[col] = new_col
    final_df.rename(columns=new_columns, inplace=True)
    logging.info("Column names standardized.")

    # 3. Standardize Data Types: Convert numerical columns to numeric type
    logging.info("Standardizing data types...")
    # Exclude known identifier columns from numeric conversion attempts
    id_cols = ['year', 'state', 'district', 'block', 'village_grampanchayath', 'village', 'gram_panchayath']
    for col in final_df.columns:
        if col not in id_cols:
            try:
                # Convert to numeric, coercing errors to NaN
                final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
                # Fill NaNs that might have been introduced by 'coerce' (e.g., if a non-numeric string was present)
                if final_df[col].isnull().any():
                    final_df[col] = final_df[col].fillna(0) # Fill with 0 for numerical columns
            except Exception as e:
                logging.debug(f"Could not convert column '{col}' to numeric. Keeping original type. Error: {e}")
    logging.info("Data types standardized.")

    # Create the processed data directory if it doesn't exist
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    output_file_path = os.path.join(PROCESSED_DATA_DIR, CONSOLIDATED_FILE_NAME)

    # Save the final consolidated DataFrame to CSV
    try:
        final_df.to_csv(output_file_path, index=False)
        logging.info(f"Consolidated data successfully saved to {output_file_path}")
    except Exception as e:
        logging.critical(f"Failed to save consolidated data to {output_file_path}: {e}")

    logging.info("Data consolidation process complete.")

if __name__ == "__main__":
    consolidate_data()

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os

# --- Configuration ---
# Directory where processed (consolidated) data is stored
PROCESSED_DATA_DIR = "data/processed"
# Name of the consolidated CSV file to be analyzed
CONSOLIDATED_FILE_NAME = "consolidated_soil_nutrient_data.csv"
# Directory where analysis results (plots) will be saved
ANALYSIS_OUTPUT_DIR = "analysis_results"
# Log file to record analysis progress and findings
LOG_FILE = "analysis_log.log"

# --- Logging Setup ---
# Configure logging to write messages to both a file and the console.
# INFO level messages and above will be recorded.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),  # Log to file
                        logging.StreamHandler()         # Log to console
                    ])

# --- Main Analysis Logic ---
def perform_eda_and_insights():
    """
    Performs Exploratory Data Analysis (EDA) on the consolidated soil nutrient data,
    generates visualizations, and extracts meaningful insights.
    """
    logging.info("Starting data analysis and insights generation.")

    # Construct the full path to the consolidated data file
    consolidated_file_path = os.path.join(PROCESSED_DATA_DIR, CONSOLIDATED_FILE_NAME)

    # Check if the consolidated data file exists
    if not os.path.exists(consolidated_file_path):
        logging.error(f"Consolidated data file not found at: {consolidated_file_path}. "
                      "Please ensure 'consolidate_data.py' was run successfully.")
        return # Exit if the file is not found

    try:
        # Load the consolidated data into a Pandas DataFrame
        df = pd.read_csv(consolidated_file_path)
        logging.info(f"Loaded consolidated data with {len(df)} rows and {len(df.columns)} columns.")
    except Exception as e:
        logging.critical(f"Error loading consolidated data from {consolidated_file_path}: {e}")
        return # Exit if data loading fails

    # Create the directory for analysis results if it does not exist
    os.makedirs(ANALYSIS_OUTPUT_DIR, exist_ok=True)

    # --- 1. Basic Data Overview ---
    logging.info("\n--- Data Overview ---")
    logging.info(f"First 5 rows:\n{df.head()}")
    logging.info(f"Column information:\n{df.info()}")
    logging.info(f"Descriptive statistics:\n{df.describe().T}") # Transpose for better readability
    logging.info(f"Number of unique years: {df['year'].nunique()}")
    logging.info(f"Unique years: {df['year'].unique()}")
    logging.info(f"Number of unique states: {df['state'].nunique()}")
    logging.info(f"Unique states: {df['state'].unique()}")
    logging.info(f"Number of unique districts: {df['district'].nunique()}")
    logging.info(f"Number of unique blocks: {df['block'].nunique()}")

    # --- 2. Identify Key Nutrient Columns ---
    # These columns are assumed to be numerical and represent nutrient values.
    # Exclude identifier columns that should not be part of numerical analysis.
    # Adjust this list based on the actual standardized column names from consolidate_data.py
    # Common macro-nutrients: Nitrogen, Phosphorus, Potassium, Organic Carbon (pH is a property)
    # Common micro-nutrients: Zinc, Iron, Manganese, Copper, Boron
    # This list is based on common soil health parameters; verify against your actual data.
    potential_nutrient_cols = [
        'ph', 'ec', 'oc_percent', 'nitrogen', 'phosphorus', 'potassium',
        'sulphur', 'zinc', 'iron', 'manganese', 'copper', 'boron'
    ]
    # Filter to only include columns actually present in the DataFrame and are numeric
    nutrient_cols = [col for col in potential_nutrient_cols if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
    logging.info(f"\nIdentified numerical nutrient columns for analysis: {nutrient_cols}")

    if not nutrient_cols:
        logging.warning("No numerical nutrient columns identified for analysis. "
                        "Please check the column naming and data types after consolidation.")
        return # Exit if no relevant columns are found for plotting

    # --- 3. Exploratory Data Analysis (EDA) and Visualizations ---

    # Set a consistent style for plots
    sns.set_style("whitegrid")
    plt.rcParams['figure.dpi'] = 100 # Set higher DPI for better quality images
    plt.rcParams['figure.figsize'] = (10, 6) # Default figure size

    # Trend of key nutrients over years (if multiple years exist)
    if df['year'].nunique() > 1:
        logging.info("\n--- Nutrient Trends Over Years ---")
        for nutrient in ['ph', 'oc_percent', 'nitrogen', 'phosphorus', 'potassium']:
            if nutrient in nutrient_cols:
                plt.figure() # Create a new figure for each plot
                sns.lineplot(data=df.groupby('year')[nutrient].mean().reset_index(), x='year', y=nutrient, marker='o')
                plt.title(f'Average {nutrient.replace("_", " ").title()} Over Years')
                plt.xlabel('Year')
                plt.ylabel(f'Average {nutrient.replace("_", " ").title()} Value')
                plt.grid(True, linestyle='--', alpha=0.7)
                plt.tight_layout()
                plt.savefig(os.path.join(ANALYSIS_OUTPUT_DIR, f'{nutrient}_trend_over_years.png'))
                plt.close() # Close the plot to free memory
                logging.info(f"  Generated {nutrient}_trend_over_years.png")
            else:
                logging.warning(f"  Nutrient '{nutrient}' not found in data for trend analysis. Skipping plot.")

    # Distribution of key nutrients
    logging.info("\n--- Distribution of Key Nutrients ---")
    for nutrient in nutrient_cols: # Iterate through all identified nutrient columns
        plt.figure()
        sns.histplot(df[nutrient], kde=True, bins=30)
        plt.title(f'Distribution of {nutrient.replace("_", " ").title()}')
        plt.xlabel(nutrient.replace("_", " ").title())
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(os.path.join(ANALYSIS_OUTPUT_DIR, f'{nutrient}_distribution.png'))
        plt.close()
        logging.info(f"  Generated {nutrient}_distribution.png")

    # Regional variation (e.g., average nutrients by State)
    logging.info("\n--- Regional Variation of Nutrients (by State) ---")
    # For pH, a box plot can show distribution across states
    if 'ph' in nutrient_cols:
        plt.figure(figsize=(14, 8)) # Wider plot for many states
        sns.boxplot(data=df, x='state', y='ph', palette='viridis')
        plt.title('pH Distribution Across States')
        plt.xlabel('State')
        plt.ylabel('pH Value')
        plt.xticks(rotation=60, ha='right') # Rotate labels for readability
        plt.tight_layout()
        plt.savefig(os.path.join(ANALYSIS_OUTPUT_DIR, 'ph_by_state_boxplot.png'))
        plt.close()
        logging.info("  Generated ph_by_state_boxplot.png")

    # For other nutrients, bar plots of averages are useful
    for nutrient in ['nitrogen', 'phosphorus', 'potassium', 'zinc', 'iron', 'boron']: # Example nutrients for state comparison
        if nutrient in nutrient_cols:
            state_avg = df.groupby('state')[nutrient].mean().sort_values(ascending=False).reset_index()
            plt.figure(figsize=(14, 8))
            sns.barplot(data=state_avg, x='state', y=nutrient, palette='coolwarm')
            plt.title(f'Average {nutrient.replace("_", " ").title()} Across States')
            plt.xlabel('State')
            plt.ylabel(f'Average {nutrient.replace("_", " ").title()} Value')
            plt.xticks(rotation=60, ha='right')
            plt.tight_layout()
            plt.savefig(os.path.join(ANALYSIS_OUTPUT_DIR, f'{nutrient}_by_state_barplot.png'))
            plt.close()
            logging.info(f"  Generated {nutrient}_by_state_barplot.png")
        else:
            logging.warning(f"  Nutrient '{nutrient}' not found for state-wise bar plot. Skipping.")


    # Correlation Matrix of Nutrients
    logging.info("\n--- Correlation Matrix of Nutrient Values ---")
    if len(nutrient_cols) > 1: # Need at least two columns for correlation
        corr_matrix = df[nutrient_cols].corr()
        plt.figure(figsize=(12, 10))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
        plt.title('Correlation Matrix of Soil Nutrients')
        plt.tight_layout()
        plt.savefig(os.path.join(ANALYSIS_OUTPUT_DIR, 'nutrient_correlation_matrix.png'))
        plt.close()
        logging.info("  Generated nutrient_correlation_matrix.png")
    else:
        logging.warning("Not enough numerical nutrient columns to generate a correlation matrix.")


    # --- 4. Identify Soil Health Patterns and Regional Trends ---
    logging.info("\n--- Key Soil Health Patterns and Regional Trends (Summary) ---")

    # Example: States with highest/lowest pH
    if 'ph' in nutrient_cols:
        avg_ph_by_state = df.groupby('state')['ph'].mean().sort_values()
        logging.info(f"  States with highest average pH (most alkaline):\n{avg_ph_by_state.tail(5)}")
        logging.info(f"  States with lowest average pH (most acidic):\n{avg_ph_by_state.head(5)}")

    # Example: States with highest/lowest Organic Carbon
    if 'oc_percent' in nutrient_cols:
        avg_oc_by_state = df.groupby('state')['oc_percent'].mean().sort_values()
        logging.info(f"  States with highest average Organic Carbon:\n{avg_oc_by_state.tail(5)}")
        logging.info(f"  States with lowest average Organic Carbon:\n{avg_oc_by_state.head(5)}")

    # Example: Identify regions with potential nutrient deficiencies (e.g., low Nitrogen)
    if 'nitrogen' in nutrient_cols:
        # Define a threshold for 'low' nitrogen (this would be domain-specific)
        # For demonstration, let's say Nitrogen < 200 kg/ha is low (hypothetical)
        low_nitrogen_regions = df[df['nitrogen'] < df['nitrogen'].quantile(0.25)] # Bottom 25th percentile
        if not low_nitrogen_regions.empty:
            logging.info(f"\n  Regions (State-District-Block) with potentially low Nitrogen levels (bottom 25th percentile):\n"
                         f"{low_nitrogen_regions[['state', 'district', 'block', 'nitrogen']].drop_duplicates().head(10)}")

    # Add more insights here based on the data and your observations
    # For instance:
    # - Distribution of micro-nutrients in specific states
    # - Correlation of pH with other nutrient availabilities
    # - Identifying blocks with extreme values (high/low) for certain nutrients

    logging.info("\nData analysis and insights generation complete. "
                 f"Check '{ANALYSIS_OUTPUT_DIR}' folder for generated visualizations.")

if __name__ == "__main__":
    perform_eda_and_insights()

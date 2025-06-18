s pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# --- Configuration ---
# Base URL of the website to scrape
BASE_URL = "https://soilhealth.dac.gov.in/piechart"
# Directory where raw scraped data will be stored
OUTPUT_RAW_DIR = "data/raw"
# Log file to record scraping progress and errors
LOG_FILE = "scraping_log.log"

# --- Logging Setup ---
# Configure logging to write messages to both a file and the console.
# INFO level messages and above will be recorded.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE),  # Log to file
                        logging.StreamHandler()         # Log to console
                    ])

# --- Helper Functions ---
def setup_driver():
    """
    Sets up and returns a Selenium WebDriver instance for Chrome.
    Uses webdriver_manager to automatically download and manage the ChromeDriver.
    """
    logging.info("Setting up Chrome WebDriver...")
    try:
        # Automatically downloads and installs the correct ChromeDriver version
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        # Uncomment the line below to run the browser in headless mode (without a visible UI)
        # options.add_argument("--headless")
        options.add_argument("--disable-gpu") # Recommended for headless mode
        options.add_argument("--no-sandbox") # Recommended for headless mode
        options.add_argument("--start-maximized") # Maximize window to ensure elements are visible and interactable
        options.add_argument("--disable-dev-shm-usage") # Overcomes limited resource problems in some environments

        driver = webdriver.Chrome(service=service, options=options)
        logging.info("WebDriver setup successful.")
        return driver
    except Exception as e:
        logging.critical(f"Failed to set up WebDriver. Please ensure Chrome is installed and check your internet connection: {e}")
        exit(1) # Exit the script if the driver cannot be set up

def get_dropdown_options(driver, element_id):
    """
    Retrieves all valid options (value and text) from a dropdown HTML element.
    Filters out common placeholder values like '0' or 'select'.
    Includes a retry mechanism for StaleElementReferenceException.
    """
    try:
        # Wait until the dropdown element is present on the page
        dropdown_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, element_id))
        )
        select = Select(dropdown_element)
        # Extract options, filtering out placeholder values
        options = [(option.get_attribute("value"), option.text.strip())
                   for option in select.options
                   if option.get_attribute("value") not in ["0", "select"] and option.text.strip() != "Select"]
        return options
    except (NoSuchElementException, TimeoutException) as e:
        logging.error(f"Dropdown with ID '{element_id}' not found or not interactable within timeout: {e}")
        return []
    except StaleElementReferenceException:
        # If the element becomes stale, wait briefly and try again recursively
        logging.warning(f"StaleElementReferenceException when getting options for {element_id}. Retrying...")
        time.sleep(1)
        return get_dropdown_options(driver, element_id)

def select_dropdown_option(driver, element_id, value):
    """
    Selects a specific option in a dropdown HTML element by its value.
    Includes a retry mechanism for StaleElementReferenceException.
    """
    try:
        # Wait until the dropdown element is present and clickable
        select_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, element_id))
        )
        select = Select(select_element)
        select.select_by_value(value)
        logging.info(f"Selected '{select.first_selected_option.text}' in '{element_id}' dropdown.")
        return True
    except (NoSuchElementException, TimeoutException) as e:
        logging.error(f"Could not select value '{value}' in dropdown '{element_id}': {e}")
        return False
    except StaleElementReferenceException:
        # If the element becomes stale, wait briefly and try again recursively
        logging.warning(f"StaleElementReferenceException when selecting {value} in {element_id}. Retrying...")
        time.sleep(1)
        return select_dropdown_option(driver, element_id, value)

def get_table_data(driver, table_id):
    """
    Extracts data from a specified HTML table identified by its ID.
    Waits for the table to be visible and present before attempting to read its content.
    """
    try:
        # Wait until the table element is visible and present on the page
        table_element = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.ID, table_id))
        )
        # Get the full HTML content of the table
        table_html = table_element.get_attribute('outerHTML')
        # Use pandas to read the HTML table. pd.read_html returns a list of DataFrames.
        # We assume the first DataFrame in the list is the one we want.
        df = pd.read_html(table_html)[0]
        return df
    except (NoSuchElementException, TimeoutException) as e:
        logging.error(f"Table with ID '{table_id}' not found or not loaded within timeout: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while extracting table '{table_id}': {e}")
        return None

def save_data(df, year, state, district, block, nutrient_type):
    """
    Saves the DataFrame to a CSV file within the specified hierarchical directory structure.
    Sanitizes state, district, and block names to be valid for file paths.
    """
    # Replace non-alphanumeric characters with underscores for safe file and directory names
    state_safe = "".join([c if c.isalnum() else "_" for c in state])
    district_safe = "".join([c if c.isalnum() else "_" for c in district])
    block_safe = "".join([c if c.isalnum() else "_" for c in block])

    # Construct the full directory path: data/raw/<year>/<state>/<district>/
    dir_path = os.path.join(OUTPUT_RAW_DIR, str(year), state_safe, district_safe)
    # Create the directory if it does not exist
    os.makedirs(dir_path, exist_ok=True)
    # Construct the file name: <block>_<nutrient_type>.csv
    file_name = f"{block_safe}_{nutrient_type}.csv"
    file_path = os.path.join(dir_path, file_name)

    try:
        # Save the DataFrame to CSV without the index
        df.to_csv(file_path, index=False)
        logging.info(f"Saved {nutrient_type} data for Block: '{block}' in '{district}', '{state}', Year: '{year}' to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save {nutrient_type} data for Block '{block}' to {file_path}: {e}")

# --- Main Scraping Logic ---
def scrape_soil_data():
    """
    Main function to orchestrate the web scraping process.
    Navigates through years, states, districts, and blocks,
    extracting and saving soil nutrient data.
    """
    driver = setup_driver() # Initialize the WebDriver
    try:
        driver.get(BASE_URL) # Open the target URL
        logging.info(f"Successfully navigated to {BASE_URL}")

        # Wait for the main elements, specifically the year dropdown, to be present on the page
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "ddlYear"))
        )
        time.sleep(1) # Small pause to allow JavaScript to fully load dropdown contents

        # Get all available years from the 'ddlYear' dropdown
        year_options = get_dropdown_options(driver, "ddlYear")
        if not year_options:
            logging.error("No years found in the dropdown. Scraping cannot proceed. Exiting.")
            return # Exit if no years are found

        # Loop through each year option
        for year_value, year_text in year_options:
            logging.info(f"\n--- Processing Year: {year_text} ---")
            # Select the current year in the dropdown
            if not select_dropdown_option(driver, "ddlYear", year_value):
                continue # Skip to next year if selection fails

            time.sleep(2) # Give time for the page to update and state dropdown to populate

            # Get all available states from the 'ddlState' dropdown
            state_options = get_dropdown_options(driver, "ddlState")
            if not state_options:
                logging.warning(f"No states found for year {year_text}. Skipping this year.")
                continue # Skip to next year if no states are found

            # Loop through each state option
            for state_value, state_text in state_options:
                logging.info(f"  Processing State: {state_text}")
                # Select the current state in the dropdown
                if not select_dropdown_option(driver, "ddlState", state_value):
                    continue # Skip to next state if selection fails

                # Wait for the district dropdown to become clickable after state selection
                WebDriverWait(driver, 20).until(
                    EC.element_to_be_clickable((By.ID, "ddlDistrict"))
                )
                time.sleep(1.5) # Additional pause for district options to load

                # Get all available districts from the 'ddlDistrict' dropdown
                district_options = get_dropdown_options(driver, "ddlDistrict")
                if not district_options:
                    logging.warning(f"    No districts found for State: {state_text} in Year: {year_text}. Skipping this state.")
                    continue # Skip to next state if no districts are found

                # Loop through each district option
                for district_value, district_text in district_options:
                    logging.info(f"    Processing District: {district_text}")
                    # Select the current district in the dropdown
                    if not select_dropdown_option(driver, "ddlDistrict", district_value):
                        continue # Skip to next district if selection fails

                    # Wait for the block dropdown to become clickable after district selection
                    WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.ID, "ddlBlock"))
                    )
                    time.sleep(1.5) # Additional pause for block options to load

                    # Get all available blocks from the 'ddlBlock' dropdown
                    block_options = get_dropdown_options(driver, "ddlBlock")
                    if not block_options:
                        logging.warning(f"      No blocks found for District: {district_text} in State: {state_text}, Year: {year_text}. Skipping this district.")
                        continue # Skip to next district if no blocks are found

                    # Loop through each block option
                    for block_value, block_text in block_options:
                        logging.info(f"      Processing Block: {block_text}")
                        try:
                            # Re-select the block to ensure the correct content loads, especially if the page reloads dynamically
                            if not select_dropdown_option(driver, "ddlBlock", block_value):
                                continue # Skip to next block if selection fails

                            time.sleep(1.5) # Wait for the nutrient tabs/tables to load after block selection

                            # --- Process MacroNutrient Data ---
                            try:
                                # Define XPath for the "Table View" button within the MacroNutrient section
                                macro_table_view_xpath = "//div[@id='MacroNutrient']//a[contains(text(),'Table View')]"
                                # Wait for the button to be clickable and then click it
                                macro_table_view_button = WebDriverWait(driver, 10).until(
                                    EC.element_to_be_clickable((By.XPATH, macro_table_view_xpath))
                                )
                                macro_table_view_button.click()
                                time.sleep(1) # Allow time for the table to render after clicking "Table View"

                                # Extract data from the 'gridMacroNutrient' table
                                macro_df = get_table_data(driver, "gridMacroNutrient")
                                if macro_df is not None and not macro_df.empty:
                                    save_data(macro_df, year_text, state_text, district_text, block_text, "macro")
                                else:
                                    logging.warning(f"        No MacroNutrient data found or table is empty for Block: {block_text}.")

                            except (NoSuchElementException, TimeoutException) as e:
                                logging.warning(f"        MacroNutrient 'Table View' button or table not found for Block: {block_text}: {e}")
                            except Exception as e:
                                logging.error(f"        Error processing MacroNutrient for Block: {block_text}: {e}")

                            # --- Process MicroNutrient Data ---
                            # First, click on the MicroNutrient tab to make its content visible and interactable
                            try:
                                micro_tab_xpath = "//a[@href='#MicroNutrient']"
                                micro_tab = WebDriverWait(driver, 5).until(
                                    EC.element_to_be_clickable((By.XPATH, micro_tab_xpath))
                                )
                                micro_tab.click()
                                time.sleep(1) # Allow time for the tab content to switch

                                # Define XPath for the "Table View" button within the MicroNutrient section
                                micro_table_view_xpath = "//div[@id='MicroNutrient']//a[contains(text(),'Table View')]"
                                # Wait for the button to be clickable and then click it
                                micro_table_view_button = WebDriverWait(driver, 10).until(
                                    EC.element_to_be_clickable((By.XPATH, micro_table_view_xpath))
                                )
                                micro_table_view_button.click()
                                time.sleep(1) # Allow time for the table to render after clicking "Table View"

                                # Extract data from the 'gridMicroNutrient' table
                                micro_df = get_table_data(driver, "gridMicroNutrient")
                                if micro_df is not None and not micro_df.empty:
                                    save_data(micro_df, year_text, state_text, district_text, block_text, "micro")
                                else:
                                    logging.warning(f"        No MicroNutrient data found or table is empty for Block: {block_text}.")

                            except (NoSuchElementException, TimeoutException) as e:
                                logging.warning(f"        MicroNutrient 'Table View' button or table not found for Block: {block_text}: {e}")
                            except Exception as e:
                                logging.error(f"        Error processing MicroNutrient for Block: {block_text}: {e}")

                            # After processing both, it's good practice to switch back to MacroNutrient tab
                            # This ensures the page is in a consistent state for the next block selection
                            try:
                                macro_tab_xpath = "//a[@href='#MacroNutrient']"
                                macro_tab = WebDriverWait(driver, 5).until(
                                    EC.element_to_be_clickable((By.XPATH, macro_tab_xpath))
                                )
                                macro_tab.click()
                                time.sleep(0.5)
                            except Exception as e:
                                logging.debug(f"Could not switch back to MacroNutrient tab after processing block {block_text}: {e}")

                        except Exception as e:
                            logging.error(f"        An unhandled error occurred while processing Block: {block_text} in {district_text}, {state_text}, {year_text}. Skipping to next block. Error: {e}")
                            # Continue to the next block even if there's a problem with the current one

    except Exception as e:
        logging.critical(f"A critical, unrecoverable error occurred during the overall scraping process: {e}")
    finally:
        logging.info("Scraping finished. Closing browser.")
        if driver: # Ensure driver exists before quitting
            driver.quit()

if __name__ == "__main__":
    # Ensure the base output directory for raw data exists before starting the scrape
    os.makedirs(OUTPUT_RAW_DIR, exist_ok=True)
    scrape_soil_data()

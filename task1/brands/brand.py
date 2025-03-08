import json
import time
import os
import concurrent.futures
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("natrue_brand_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Constants
BASE_URL = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=brands"
# Updated URL template for pagination - FIXED to match actual site structure
PAGE_URL_TEMPLATE = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=brands&prod[pageIndex]=16&prod[search]=&brands[pageNumber]={}&brands[filters][letter]="
ESTIMATED_TOTAL_PAGES = 12  # There are 12 pages as mentioned
JSON_FILE = "natrue_brand_details.json"
EXCEL_FILE = "natrue_brand_details.xlsx"
CSV_FILE = "natrue_brand_details.csv"
TEMP_DIR = "temp_brand_files"
PROCESSED_BRANDS_FILE = "processed_brands.json"

# Initialize files and directories
def initialize_files():
    # Initialize JSON file
    if not os.path.exists(JSON_FILE):
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump({"brands": []}, f, indent=4)
    
    # Initialize Excel file
    if not os.path.exists(EXCEL_FILE):
        columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
        df = pd.DataFrame(columns=columns)
        df.to_excel(EXCEL_FILE, sheet_name="Brand Details", index=False)
    
    # Initialize CSV file
    if not os.path.exists(CSV_FILE):
        columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
        df = pd.DataFrame(columns=columns)
        df.to_csv(CSV_FILE, index=False)
    
    # Create temp directory if it doesn't exist
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
    
    # Initialize processed brands tracker
    if not os.path.exists(PROCESSED_BRANDS_FILE):
        with open(PROCESSED_BRANDS_FILE, "w", encoding="utf-8") as f:
            json.dump({"processed_brands": []}, f, indent=4)
    
    logger.info("Files and directories initialized successfully.")

# Function to get already processed brands
def get_processed_brands():
    try:
        if os.path.exists(PROCESSED_BRANDS_FILE):
            with open(PROCESSED_BRANDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("processed_brands", []))
        return set()
    except Exception as e:
        logger.error(f"Error loading processed brands: {e}")
        return set()

# Function to add brand to processed brands list
def add_to_processed_brands(brand_name):
    try:
        processed_brands = get_processed_brands()
        
        # If brand already processed, just return
        if brand_name in processed_brands:
            return
        
        processed_brands.add(brand_name)
        
        with open(PROCESSED_BRANDS_FILE, "w", encoding="utf-8") as f:
            json.dump({"processed_brands": list(processed_brands)}, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Added '{brand_name}' to processed brands list")
    except Exception as e:
        logger.error(f"Error updating processed brands: {e}")

# Set up the Selenium WebDriver with optimized settings
def setup_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-notifications")
    options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images
    options.add_argument("--headless")  # Run in headless mode for speed
    options.page_load_strategy = 'eager'  # Load DOM without waiting for resources
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(30)
    return driver

# Function to append brand data to JSON
def append_to_json(brand_data):
    try:
        max_retries = 5
        retries = 0
        
        while retries < max_retries:
            try:
                # Read existing data
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Check if brand already exists in the data
                brand_exists = False
                for brand in data["brands"]:
                    if brand["name"] == brand_data["name"]:
                        brand_exists = True
                        break
                
                # Only append if brand doesn't exist
                if not brand_exists:
                    data["brands"].append(brand_data)
                    
                    # Write back to file
                    with open(JSON_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    
                    logger.info(f"Appended brand '{brand_data['name']}' to JSON file")
                else:
                    logger.info(f"Skipped duplicate brand '{brand_data['name']}' in JSON file")
                
                break
            except (json.JSONDecodeError, FileNotFoundError) as e:
                retries += 1
                logger.warning(f"JSON retry {retries}: {e}")
                time.sleep(0.5)  # Short wait before retrying
                
    except Exception as e:
        logger.error(f"Error appending to JSON: {e}")

# Function to check if brand already exists in Excel
def brand_exists_in_excel(brand_name):
    try:
        if os.path.exists(EXCEL_FILE):
            df = pd.read_excel(EXCEL_FILE)
            return brand_name in df["name"].values
        return False
    except Exception as e:
        logger.error(f"Error checking Excel for brand '{brand_name}': {e}")
        return False

# Function to append brand data to Excel through temp files
def append_to_excel(brand_data):
    try:
        # First check if brand already exists
        if brand_exists_in_excel(brand_data['name']):
            logger.info(f"Skipped duplicate brand '{brand_data['name']}' - already in Excel")
            return
        
        # Create a safe filename from brand name
        safe_name = ''.join(c if c.isalnum() else '_' for c in brand_data['name'])
        safe_name = safe_name[:50]  # Limit filename length
        
        # Create a unique temp filename with timestamp
        temp_filename = os.path.join(TEMP_DIR, f"temp_{safe_name}_{int(time.time())}.csv")
        
        # Save brand data to temp CSV file
        temp_df = pd.DataFrame([brand_data])
        temp_df.to_csv(temp_filename, index=False)
        
        logger.info(f"Saved brand '{brand_data['name']}' to temp file {temp_filename}")
    except Exception as e:
        logger.error(f"Error saving temp data: {e}")

# Merge all temp files into Excel and CSV
def merge_temp_files():
    try:
        # Get all temp CSV files
        if not os.path.exists(TEMP_DIR):
            logger.warning("Temp directory not found")
            return
            
        temp_files = [os.path.join(TEMP_DIR, f) for f in os.listdir(TEMP_DIR) if f.startswith("temp_") and f.endswith(".csv")]
        
        if not temp_files:
            logger.warning("No temp files found to merge")
            return
            
        logger.info(f"Found {len(temp_files)} temp files to merge")
        
        # Load existing Excel if it exists
        if os.path.exists(EXCEL_FILE):
            try:
                existing_df = pd.read_excel(EXCEL_FILE)
                logger.info(f"Loaded existing Excel file with {len(existing_df)} records")
            except Exception as e:
                logger.error(f"Error reading existing Excel file: {e}")
                columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
                existing_df = pd.DataFrame(columns=columns)
        else:
            columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
            existing_df = pd.DataFrame(columns=columns)
        
        # Load existing CSV if it exists
        if os.path.exists(CSV_FILE):
            try:
                existing_csv_df = pd.read_csv(CSV_FILE)
                logger.info(f"Loaded existing CSV file with {len(existing_csv_df)} records")
            except Exception as e:
                logger.error(f"Error reading existing CSV file: {e}")
                columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
                existing_csv_df = pd.DataFrame(columns=columns)
        else:
            columns = ["name", "company", "address", "country", "website", "additional_info", "page_number"]
            existing_csv_df = pd.DataFrame(columns=columns)
        
        # Load and concatenate all temp files
        dfs = []
        successful_files = []
        
        for temp_file in temp_files:
            try:
                df = pd.read_csv(temp_file)
                if not df.empty:
                    dfs.append(df)
                    successful_files.append(temp_file)
                else:
                    logger.warning(f"Empty temp file: {temp_file}")
            except Exception as e:
                logger.error(f"Error processing temp file {temp_file}: {e}")
        
        # Only proceed if we have new data to add
        if dfs:
            # Merge all new dataframes
            new_df = pd.concat(dfs, ignore_index=True)
            
            # De-duplicate new data based on brand name
            new_df = new_df.drop_duplicates(subset=["name"])
            
            # Get existing brand names for Excel
            existing_names = set(existing_df["name"].values) if not existing_df.empty else set()
            
            # Filter out brands that already exist in the Excel file
            filtered_df = new_df[~new_df["name"].isin(existing_names)]
            
            # Get existing brand names for CSV
            existing_csv_names = set(existing_csv_df["name"].values) if not existing_csv_df.empty else set()
            
            # Filter out brands that already exist in the CSV file
            filtered_csv_df = new_df[~new_df["name"].isin(existing_csv_names)]
            
            if not filtered_df.empty:
                # Merge with existing data for Excel
                merged_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                
                # Save to Excel with error handling
                try:
                    merged_df.to_excel(EXCEL_FILE, sheet_name="Brand Details", index=False)
                    logger.info(f"Successfully saved {len(merged_df)} records to Excel file " 
                                f"(added {len(filtered_df)} new records)")
                except Exception as e:
                    logger.error(f"Error saving to Excel: {e}")
            else:
                logger.info("No new unique brands to add to Excel")
            
            if not filtered_csv_df.empty:
                # Merge with existing data for CSV
                merged_csv_df = pd.concat([existing_csv_df, filtered_csv_df], ignore_index=True)
                
                # Save to CSV with error handling
                try:
                    merged_csv_df.to_csv(CSV_FILE, index=False)
                    logger.info(f"Successfully saved {len(merged_csv_df)} records to CSV file " 
                                f"(added {len(filtered_csv_df)} new records)")
                except Exception as e:
                    logger.error(f"Error saving to CSV: {e}")
            else:
                logger.info("No new unique brands to add to CSV")
            
            # Remove processed temp files
            for temp_file in successful_files:
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logger.error(f"Error removing temp file {temp_file}: {e}")
        else:
            logger.warning("No new data to add")
        
    except Exception as e:
        logger.error(f"Error in merge_temp_files: {e}")

# Extract information from brand details
def extract_brand_details(brand_soup, brand_name, page_number):
    try:
        # 1. Get brand name
        name = brand_name
        
        # 2. Get company info and address
        company = ""
        address = ""
        country = ""
        website = ""
        additional_info = ""
        
        # Get info div which contains all the details
        info_div = brand_soup.find("div", class_="dialog-brand__info")
        if info_div:
            info_content = info_div.get_text(strip=True, separator=" ")
            
            # Extract website if available
            website_link = info_div.find("a")
            if website_link and 'href' in website_link.attrs:
                website = website_link.get_text(strip=True)
            
            # Process info text to extract different components
            if info_content:
                # Try to identify company name and address parts
                lines = [line.strip() for line in info_div.get_text(separator="|").split("|") if line.strip()]
                
                if lines:
                    # First line is usually the company name
                    company = lines[0].strip()
                    
                    # Try to extract country from address
                    address_parts = []
                    country_candidates = ["Italy", "Germany", "France", "Spain", "USA", "UK", "Switzerland", 
                                         "Austria", "Belgium", "Netherlands", "Denmark", "Sweden", "Norway",
                                         "Finland", "Portugal", "Greece", "Ireland", "Poland", "Hungary",
                                         "Czech Republic", "Japan", "China", "Australia", "Canada", "Brazil"]
                    
                    found_country = False
                    for i in range(1, len(lines)):
                        if any(country in lines[i] for country in country_candidates):
                            found_country = True
                            # Split this line to get country
                            for country_name in country_candidates:
                                if country_name in lines[i]:
                                    # Extract country
                                    country = country_name
                                    # Remove country from this part and add to address
                                    address_part = lines[i].replace(country_name, "").strip()
                                    if address_part:
                                        address_parts.append(address_part)
                                    break
                        else:
                            # If no country in this line, it's part of the address
                            address_parts.append(lines[i])
                    
                    # Join all address parts
                    address = " ".join(address_parts)
                    
                    # If website was in the text, remove it from address
                    if website and website in address:
                        address = address.replace(website, "").strip()
                
                # Extract any additional information if available
                additional_info_div = brand_soup.find("div", class_="dialog-brand__description")
                if additional_info_div:
                    additional_info = additional_info_div.get_text(strip=True)
        
        # Create a dictionary with all the extracted information
        brand_info = {
            "name": name,
            "company": company,
            "address": address,
            "country": country,
            "website": website,
            "additional_info": additional_info,
            "page_number": page_number
        }
        
        return brand_info
    except Exception as e:
        logger.error(f"Error extracting brand details: {e}")
        # Return basic brand info in case of error
        return {
            "name": brand_name,
            "company": "Error extracting data",
            "address": "",
            "country": "",
            "website": "",
            "additional_info": f"Error: {str(e)}",
            "page_number": page_number
        }

# Function to process a single brand
def process_brand(driver, brand_link, page_number, processed_brands):
    try:
        # Get brand name before clicking
        brand_name = brand_link.text.strip()
        
        # Skip if brand already processed
        if brand_name in processed_brands:
            logger.info(f"Skipping already processed brand: {brand_name}")
            return True
        
        logger.info(f"Processing new brand: {brand_name} on page {page_number}")
        
        # Scroll to element before clicking
        driver.execute_script("arguments[0].scrollIntoView();", brand_link)
        time.sleep(0.5)
        
        # Click using JavaScript to bypass overlay issues
        driver.execute_script("arguments[0].click();", brand_link)
        
        # Wait for the dialog to appear with shorter timeout
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "dialog-brand"))
        )
        
        # Extract brand details from the brand page
        brand_soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract brand details
        brand_info = extract_brand_details(brand_soup, brand_name, page_number)
        
        # Save brand details immediately to JSON and temp file
        append_to_json(brand_info)
        append_to_excel(brand_info)
        
        # Mark brand as processed
        add_to_processed_brands(brand_name)
        
        # Close the dialog
        try:
            # Try to find close button
            close_button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".el-dialog__close"))
            )
            driver.execute_script("arguments[0].click();", close_button)
        except:
            # If close button not found, try pressing ESC key
            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        
        time.sleep(0.5)
        
        return True
    except Exception as e:
        logger.error(f"Error processing brand {brand_link.text.strip() if hasattr(brand_link, 'text') else 'unknown'}: {e}")
        # Try to close any open dialogs
        try:
            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.5)
        except:
            pass
        return False

# Function to process all brands on a single page
def process_page(page_number):
    driver = None
    try:
        # Get list of already processed brands
        processed_brands = get_processed_brands()
        
        driver = setup_driver()
        url = PAGE_URL_TEMPLATE.format(page_number)
        
        logger.info(f"Processing page {page_number}: {url}")
        driver.get(url)
        
        # Wait for page to load with brands
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "brand-list__item__name"))
        )
        
        # Get all brand links on this page
        brand_links = driver.find_elements(By.CLASS_NAME, "brand-list__item__name")
        
        # Count unprocessed brands
        brand_names = [link.text.strip() for link in brand_links]
        new_brands = [name for name in brand_names if name not in processed_brands]
        
        logger.info(f"Found {len(brand_links)} brands on page {page_number}, {len(new_brands)} are new")
        
        # Skip page if all brands already processed
        if not new_brands:
            logger.info(f"Skipping page {page_number} - all brands already processed")
            return 0
        
        # Process each brand
        new_processed = 0
        for i, brand_link in enumerate(brand_links):
            successful = process_brand(driver, brand_link, page_number, processed_brands)
            
            if successful and brand_link.text.strip() in new_brands:
                new_processed += 1
            
            if not successful and i < len(brand_links) - 1:
                # If processing failed, reload the page and get fresh references
                driver.get(url)
                time.sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "brand-list__item__name"))
                )
                brand_links = driver.find_elements(By.CLASS_NAME, "brand-list__item__name")
        
        # Merge temp files after processing the page
        merge_temp_files()
        
        return new_processed
    except Exception as e:
        logger.error(f"Error processing page {page_number}: {e}")
        return 0
    finally:
        if driver:
            driver.quit()

# Check if pagination works correctly
def check_pagination():
    driver = None
    try:
        driver = setup_driver()
        
        # Test page 1
        url1 = PAGE_URL_TEMPLATE.format(1)
        driver.get(url1)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "brand-list__item__name"))
        )
        brands_page1 = [link.text.strip() for link in driver.find_elements(By.CLASS_NAME, "brand-list__item__name")]
        
        # Test page 2
        url2 = PAGE_URL_TEMPLATE.format(2)
        driver.get(url2)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "brand-list__item__name"))
        )
        brands_page2 = [link.text.strip() for link in driver.find_elements(By.CLASS_NAME, "brand-list__item__name")]
        
        # Check if we got different brands
        if len(set(brands_page1) & set(brands_page2)) < len(brands_page1) * 0.9:
            logger.info(f"Pagination check: SUCCESS - Different brands found on page 1 and page 2")
            return True
        else:
            logger.warning(f"Pagination check: FAILED - Same brands found on different pages")
            # Try to analyze pagination element
            pagination_element = driver.find_element(By.CLASS_NAME, "el-pagination")
            logger.info(f"Pagination HTML: {pagination_element.get_attribute('outerHTML')}")
            return False
    except Exception as e:
        logger.error(f"Error checking pagination: {e}")
        return False
    finally:
        if driver:
            driver.quit()

# Manual experiment to find correct pagination URL
def find_pagination_url():
    driver = None
    try:
        driver = setup_driver()
        
        # First get the base page
        driver.get(BASE_URL)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "el-pagination"))
        )
        
        # Get current URL
        initial_url = driver.current_url
        logger.info(f"Initial URL: {initial_url}")
        
        # Try to click page 2 button
        page_buttons = driver.find_elements(By.CSS_SELECTOR, ".el-pager li")
        if len(page_buttons) > 1:
            # Click on the second button (page 2)
            driver.execute_script("arguments[0].click();", page_buttons[1])
            time.sleep(3)
            
            # Get the new URL
            new_url = driver.current_url
            logger.info(f"Page 2 URL: {new_url}")
            
            # Extract the pattern
            return initial_url, new_url
        
        return None, None
    except Exception as e:
        logger.error(f"Error finding pagination URL: {e}")
        return None, None
    finally:
        if driver:
            driver.quit()

# Function to determine total number of pages dynamically
def get_total_pages():
    driver = None
    try:
        driver = setup_driver()
        driver.get(PAGE_URL_TEMPLATE.format(1))  # Use the first page URL
        
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "el-pagination"))
        )
        
        # Try to find pagination info
        pagination = driver.find_element(By.CLASS_NAME, "el-pagination")
        pagination_text = pagination.text
        
        # Try to extract total pages from pagination text
        match = re.search(r'(\d+)\s*pages', pagination_text, re.IGNORECASE)
        if match:
            total_pages = int(match.group(1))
            logger.info(f"Detected {total_pages} total pages")
            return total_pages
        
        # Try to find the last page button
        last_page_buttons = driver.find_elements(By.CSS_SELECTOR, ".el-pager li")
        if last_page_buttons:
            # Get the text of the last pagination button
            last_page_num = last_page_buttons[-1].text.strip()
            if last_page_num.isdigit():
                total_pages = int(last_page_num)
                logger.info(f"Detected {total_pages} total pages from pagination buttons")
                return total_pages
        
        # If we can't determine, return the estimated value
        logger.info(f"Could not detect total pages, using estimated value: {ESTIMATED_TOTAL_PAGES}")
        return ESTIMATED_TOTAL_PAGES
    except Exception as e:
        logger.error(f"Error determining total pages: {e}")
        return ESTIMATED_TOTAL_PAGES
    finally:
        if driver:
            driver.quit()

# Force merge all temp files to ensure data is not lost
def force_merge_all_files():
    try:
        logger.info("Forcing merge of all temp files...")
        merge_temp_files()
        
        # Additional check to ensure all data is in Excel and CSV
        if os.path.exists(JSON_FILE) and os.path.exists(EXCEL_FILE) and os.path.exists(CSV_FILE):
            try:
                # Load JSON data
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    json_data = json.load(f)
                json_brands = json_data.get("brands", [])
                
                # Load Excel data
                excel_df = pd.read_excel(EXCEL_FILE)
                
                # Load CSV data
                csv_df = pd.read_csv(CSV_FILE)
                
                logger.info(f"Data count check: JSON: {len(json_brands)}, Excel: {len(excel_df)}, CSV: {len(csv_df)}")
                
                # If Excel or CSV has fewer records than JSON, do a full refresh
                if len(excel_df) < len(json_brands) or len(csv_df) < len(json_brands):
                    logger.info("Data inconsistency detected. Refreshing Excel and CSV from JSON...")
                    
                    # Convert JSON data to DataFrame
                    json_df = pd.DataFrame(json_brands)
                    
                    # Save to Excel and CSV
                    json_df.to_excel(EXCEL_FILE, sheet_name="Brand Details", index=False)
                    json_df.to_csv(CSV_FILE, index=False)
                    
                    logger.info(f"Excel and CSV files refreshed with {len(json_brands)} records")
            except Exception as e:
                logger.error(f"Error in data consistency check: {e}")
    except Exception as e:
        logger.error(f"Error in force_merge_all_files: {e}")

# Main function to extract brands from all pages
def extract_all_brands():
    try:
        # Initialize files first
        initialize_files()
        
        # Check if pagination URLs work correctly
        logger.info("Testing pagination URLs...")
        pagination_works = check_pagination()
        
        if not pagination_works:
            # Try to discover the correct pagination URL format
            logger.info("Attempting to discover correct pagination URL format...")
            page1_url, page2_url = find_pagination_url()
            
            if page1_url and page2_url and page1_url != page2_url:
                logger.info(f"Discovered pagination URLs:\nPage 1: {page1_url}\nPage 2: {page2_url}")
                
                # Extract pattern differences to determine format
                # For demonstration, we'll keep using the updated format
        
        # Get total number of pages
        total_pages = get_total_pages()
        
        total_brands = 0
        
        # Process pages sequentially to avoid overwhelming the server
        for page in range(1, total_pages + 1):
            logger.info(f"Processing page {page} of {total_pages}")
            brands_count = process_page(page)
            total_brands += brands_count
            logger.info(f"Page {page} completed with {brands_count} new brands. Running total: {total_brands}")
            
            # Short pause between pages to avoid being blocked
            time.sleep(2)
        
        # Final merge of any remaining temp files
        logger.info("Performing final merge of temp files...")
        force_merge_all_files()
        
        logger.info(f"Extraction complete. Total new brands scraped: {total_brands}")
    
    except Exception as e:
        logger.error(f"Error in main extraction process: {e}")
        # Try one last merge in case of errors
        try:
            force_merge_all_files()
        except:
            pass

if __name__ == "__main__":
    try:
        start_time = time.time()
        extract_all_brands()
        elapsed_time = time.time() - start_time
        logger.info(f"Scraping completed in {elapsed_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Attempt to merge data before exiting
        try:
            force_merge_all_files()
        except:
            pass
import json
import time
import os
import concurrent.futures
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
        logging.FileHandler("natrue_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Constants
BASE_URL = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=products"
PAGE_URL_TEMPLATE = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=products&prod[pageIndex]={}&prod[search]="
TOTAL_PAGES = 150
JSON_FILE = "natrue_product_details.json"
EXCEL_FILE = "natrue_product_details.xlsx"
TEMP_DIR = "temp_files"
PROCESSED_PRODUCTS_FILE = "processed_products.json"  # Track processed products

# Initialize files
def initialize_files():
    # Initialize JSON file
    if not os.path.exists(JSON_FILE):
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump({"products": []}, f, indent=4)
    
    # Initialize Excel file
    if not os.path.exists(EXCEL_FILE):
        columns = ["name", "brand", "manufacturer", "certification_level", 
                  "certification_description", "ingredients", 
                  "product_description", "usage", "image_url", "page_number"]
        df = pd.DataFrame(columns=columns)
        df.to_excel(EXCEL_FILE, sheet_name="Product Details", index=False)
    
    # Create temp directory if it doesn't exist
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
    
    # Initialize processed products tracker
    if not os.path.exists(PROCESSED_PRODUCTS_FILE):
        with open(PROCESSED_PRODUCTS_FILE, "w", encoding="utf-8") as f:
            json.dump({"processed_products": []}, f, indent=4)
    
    logger.info("Files initialized successfully.")

# Function to get already processed products
def get_processed_products():
    try:
        if os.path.exists(PROCESSED_PRODUCTS_FILE):
            with open(PROCESSED_PRODUCTS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("processed_products", []))
        return set()
    except Exception as e:
        logger.error(f"Error loading processed products: {e}")
        return set()

# Function to add product to processed products list
def add_to_processed_products(product_name):
    try:
        processed_products = get_processed_products()
        
        # If product already processed, just return
        if product_name in processed_products:
            return
        
        processed_products.add(product_name)
        
        with open(PROCESSED_PRODUCTS_FILE, "w", encoding="utf-8") as f:
            json.dump({"processed_products": list(processed_products)}, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Added '{product_name}' to processed products list")
    except Exception as e:
        logger.error(f"Error updating processed products: {e}")

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

# Function to append product data to JSON
def append_to_json(product_data):
    try:
        # Use file locking to avoid race conditions with concurrent processing
        # For simplicity, we're using a basic approach here
        max_retries = 5
        retries = 0
        
        while retries < max_retries:
            try:
                # Read existing data
                with open(JSON_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Check if product already exists in the data
                product_exists = False
                for product in data["products"]:
                    if product["name"] == product_data["name"]:
                        product_exists = True
                        break
                
                # Only append if product doesn't exist
                if not product_exists:
                    data["products"].append(product_data)
                    
                    # Write back to file
                    with open(JSON_FILE, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    
                    logger.info(f"Appended product '{product_data['name']}' to JSON file")
                else:
                    logger.info(f"Skipped duplicate product '{product_data['name']}' in JSON file")
                
                break
            except (json.JSONDecodeError, FileNotFoundError) as e:
                retries += 1
                logger.warning(f"JSON retry {retries}: {e}")
                time.sleep(0.5)  # Short wait before retrying
                
    except Exception as e:
        logger.error(f"Error appending to JSON: {e}")

# Function to check if product already exists in Excel
def product_exists_in_excel(product_name):
    try:
        if os.path.exists(EXCEL_FILE):
            df = pd.read_excel(EXCEL_FILE)
            return product_name in df["name"].values
        return False
    except Exception as e:
        logger.error(f"Error checking Excel for product '{product_name}': {e}")
        return False

# Function to append product data to Excel through temp files
def append_to_excel(product_data):
    try:
        # First check if product already exists in main Excel file
        if product_exists_in_excel(product_data['name']):
            logger.info(f"Skipped duplicate product '{product_data['name']}' - already in Excel")
            return
        
        # Create a safe filename from product name
        safe_name = ''.join(c if c.isalnum() else '_' for c in product_data['name'])
        safe_name = safe_name[:50]  # Limit filename length
        
        # Create a unique temp filename with timestamp to avoid collisions
        temp_filename = os.path.join(TEMP_DIR, f"temp_{safe_name}_{int(time.time())}.csv")
        
        # Save product data to temp CSV file
        temp_df = pd.DataFrame([product_data])
        temp_df.to_csv(temp_filename, index=False)
        
        logger.info(f"Saved product '{product_data['name']}' to temp file {temp_filename}")
    except Exception as e:
        logger.error(f"Error saving temp data: {e}")

# Merge all temp files into Excel
def merge_temp_files_to_excel():
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
                columns = ["name", "brand", "manufacturer", "certification_level", 
                          "certification_description", "ingredients", 
                          "product_description", "usage", "image_url", "page_number"]
                existing_df = pd.DataFrame(columns=columns)
        else:
            columns = ["name", "brand", "manufacturer", "certification_level", 
                      "certification_description", "ingredients", 
                      "product_description", "usage", "image_url", "page_number"]
            existing_df = pd.DataFrame(columns=columns)
        
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
            
            # De-duplicate new data based on product name
            new_df = new_df.drop_duplicates(subset=["name"])
            
            # Get existing product names
            existing_names = set(existing_df["name"].values) if not existing_df.empty else set()
            
            # Filter out products that already exist in the Excel file
            filtered_df = new_df[~new_df["name"].isin(existing_names)]
            
            if not filtered_df.empty:
                # Merge with existing data
                merged_df = pd.concat([existing_df, filtered_df], ignore_index=True)
                
                # Save to Excel with error handling
                try:
                    merged_df.to_excel(EXCEL_FILE, sheet_name="Product Details", index=False)
                    logger.info(f"Successfully saved {len(merged_df)} records to Excel file " 
                                f"(added {len(filtered_df)} new records)")
                    
                    # Remove successfully processed temp files
                    for temp_file in successful_files:
                        try:
                            os.remove(temp_file)
                        except Exception as e:
                            logger.error(f"Error removing temp file {temp_file}: {e}")
                except Exception as e:
                    logger.error(f"Error saving to Excel: {e}")
                    
                    # Try saving as CSV as fallback
                    try:
                        csv_backup = EXCEL_FILE.replace('.xlsx', '.csv')
                        merged_df.to_csv(csv_backup, index=False)
                        logger.info(f"Saved backup to CSV: {csv_backup}")
                    except Exception as csv_e:
                        logger.error(f"Even CSV backup failed: {csv_e}")
            else:
                logger.info("No new unique products to add to Excel")
                
                # Remove processed temp files
                for temp_file in successful_files:
                    try:
                        os.remove(temp_file)
                    except Exception as e:
                        logger.error(f"Error removing temp file {temp_file}: {e}")
        else:
            logger.warning("No new data to add to Excel")
        
    except Exception as e:
        logger.error(f"Error in merge_temp_files_to_excel: {e}")

# Function to extract product details based on the specific HTML structure
def extract_product_details(product_soup, product_name, page_number):
    try:
        # Extract product information based on the provided HTML structure
        
        # 1. Get product name
        name = product_name
        
        # 2. Get certification level and description
        certification_level = ""
        certification_description = ""
        certification_div = product_soup.find("div", class_="dialog-product__certification")
        if certification_div:
            level_div = certification_div.find("div", class_="dialog-product__certification__level")
            if level_div:
                certification_level = level_div.text.strip()
            
            desc_div = certification_div.find("div", class_="dialog-product__certification__description")
            if desc_div:
                certification_description = desc_div.text.strip()
        
        # 3. Get brand and manufacturer
        brand = ""
        manufacturer = ""
        info_div = product_soup.find("div", class_="dialog-product__info")
        if info_div:
            info_contents = info_div.find_all("div", class_="dialog-product__info__content")
            if len(info_contents) >= 1:
                brand = info_contents[0].text.strip()
            if len(info_contents) >= 2:
                manufacturer = info_contents[1].text.strip()
        
        # 4. Get product description
        description_div = product_soup.find("div", class_="dialog-product__description")
        full_description = description_div.text.strip() if description_div else ""
        
        # Parse the description to extract ingredients, description, and usage
        ingredients = ""
        product_description = ""
        usage = ""
        
        if full_description:
            # More sophisticated parsing - handle different formats
            if "Ingredients" in full_description:
                ingredients_start = full_description.find("Ingredients")
                description_start = full_description.find("Description", ingredients_start)
                usage_start = full_description.find("Usage", description_start)
                
                if description_start > 0:
                    ingredients = full_description[ingredients_start:description_start].replace("Ingredients", "", 1).strip()
                else:
                    ingredients = full_description[ingredients_start:].replace("Ingredients", "", 1).strip()
                
                if description_start > 0 and usage_start > 0:
                    product_description = full_description[description_start:usage_start].replace("Description", "", 1).strip()
                elif description_start > 0:
                    product_description = full_description[description_start:].replace("Description", "", 1).strip()
                
                if usage_start > 0:
                    usage = full_description[usage_start:].replace("Usage", "", 1).strip()
            else:
                # If structured headers aren't found, store everything in product_description
                product_description = full_description
        
        # 5. Get image URL if available
        image_url = ""
        image_tag = product_soup.find("img", class_="image-magnifier__img")
        if image_tag and 'src' in image_tag.attrs:
            image_url = image_tag['src']
        
        # Create a dictionary with all the extracted information
        product_info = {
            "name": name,
            "brand": brand,
            "manufacturer": manufacturer,
            "certification_level": certification_level,
            "certification_description": certification_description,
            "ingredients": ingredients,
            "product_description": product_description,
            "usage": usage,
            "image_url": image_url,
            "page_number": page_number
        }
        
        return product_info
    except Exception as e:
        logger.error(f"Error extracting product details: {e}")
        # Return basic product info in case of error
        return {
            "name": product_name,
            "brand": "Error extracting data",
            "manufacturer": "",
            "certification_level": "",
            "certification_description": "",
            "ingredients": "",
            "product_description": f"Error: {str(e)}",
            "usage": "",
            "image_url": "",
            "page_number": page_number
        }

# Function to process a single product
def process_product(driver, product_link, page_number, processed_products):
    try:
        # Get product name before clicking
        product_name = product_link.text.strip()
        
        # Skip if product already processed
        if product_name in processed_products:
            logger.info(f"Skipping already processed product: {product_name}")
            return True
        
        logger.info(f"Processing new product: {product_name} on page {page_number}")
        
        # Scroll to element before clicking
        driver.execute_script("arguments[0].scrollIntoView();", product_link)
        time.sleep(0.5)
        
        # Click using JavaScript to bypass overlay issues
        driver.execute_script("arguments[0].click();", product_link)
        
        # Wait for the dialog to appear with shorter timeout
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "dialog-product"))
        )
        
        # Extract product details from the product page
        product_soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Extract product details
        product_info = extract_product_details(product_soup, product_name, page_number)
        
        # Save product details immediately to JSON and temp file
        append_to_json(product_info)
        append_to_excel(product_info)
        
        # Mark product as processed
        add_to_processed_products(product_name)
        
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
        logger.error(f"Error processing product {product_name}: {e}")
        # Try to close any open dialogs
        try:
            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(0.5)
        except:
            pass
        return False

# Function to process all products on a single page
def process_page(page_number):
    driver = None
    try:
        # Get list of already processed products
        processed_products = get_processed_products()
        
        driver = setup_driver()
        url = PAGE_URL_TEMPLATE.format(page_number)
        
        logger.info(f"Processing page {page_number}: {url}")
        driver.get(url)
        
        # Wait for page to load with products
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "product-list__item__name"))
        )
        
        # Get all product links on this page
        product_links = driver.find_elements(By.CLASS_NAME, "product-list__item__name")
        
        # Count unprocessed products
        product_names = [link.text.strip() for link in product_links]
        new_products = [name for name in product_names if name not in processed_products]
        
        logger.info(f"Found {len(product_links)} products on page {page_number}, {len(new_products)} are new")
        
        # Skip page if all products already processed
        if not new_products:
            logger.info(f"Skipping page {page_number} - all products already processed")
            return 0
        
        # Process each product
        new_processed = 0
        for i, product_link in enumerate(product_links):
            successful = process_product(driver, product_link, page_number, processed_products)
            
            if successful:
                new_processed += 1
            
            if not successful and i < len(product_links) - 1:
                # If processing failed, reload the page and get fresh references
                driver.get(url)
                time.sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "product-list__item__name"))
                )
                product_links = driver.find_elements(By.CLASS_NAME, "product-list__item__name")
        
        # Merge temp files to Excel after processing the page
        merge_temp_files_to_excel()
        
        return new_processed
    except Exception as e:
        logger.error(f"Error processing page {page_number}: {e}")
        return 0
    finally:
        if driver:
            driver.quit()

# Main function to extract products from all pages
def extract_all_products():
    try:
        # Initialize files first
        initialize_files()
        
        total_products = 0
        
        # For parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(process_page, page) for page in range(1, TOTAL_PAGES + 1)]
            for future in concurrent.futures.as_completed(futures):
                products_count = future.result()
                total_products += products_count
                logger.info(f"Page completed with {products_count} new products. Running total: {total_products}")
        
        # Final merge of any remaining temp files
        logger.info("Performing final merge of temp files...")
        merge_temp_files_to_excel()
        
        logger.info(f"Extraction complete. Total new products scraped: {total_products}")
    
    except Exception as e:
        logger.error(f"Error in main extraction process: {e}")
        # Try one last merge in case of errors
        try:
            merge_temp_files_to_excel()
        except:
            pass

if __name__ == "__main__":
    try:
        start_time = time.time()
        extract_all_products()
        elapsed_time = time.time() - start_time
        logger.info(f"Scraping completed in {elapsed_time:.2f} seconds")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        # Attempt to merge data before exiting
        try:
            merge_temp_files_to_excel()
        except:
            pass
import os
import time
import logging
import threading
import concurrent.futures
from queue import Queue
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

class NewDirectionsScraper:
    """Scrapes product details from multiple pages and saves each product as a text file."""

    def __init__(self):
        """Initialize scraper settings."""
        self.config = {
            'base_url': 'https://www.newdirectionsaromatics.com/category/raw-materials/',
            'output_dir': 'product_details',  # Save text files in this folder
            'timeout': 60,
            'headless': False,
            'max_workers': 2,
        }

        os.makedirs(self.config['output_dir'], exist_ok=True)
        self.setup_logging()
        self.product_queue = Queue()
        self.lock = threading.Lock()
        self.chrome_driver_path = ChromeDriverManager().install()  # ✅ Install WebDriver only ONCE

        self.logger.info("Scraper initialized.")

    def setup_logging(self):
        """Setup logging."""
        log_file = f'scraper_{time.strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
        self.logger = logging.getLogger('NewDirectionsScraper')

    def get_browser(self):
        """Setup Selenium WebDriver without redundant WebDriver checks."""
        options = Options()
        options.add_argument("--headless")  # Run in headless mode
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")

        driver = webdriver.Chrome(service=Service(self.chrome_driver_path), options=options)
        driver.set_page_load_timeout(self.config['timeout'])
        return driver

    def scrape_category_pages(self):
        """Scrape all category pages to get product URLs."""
        driver = self.get_browser()
        page_number = 1  # Start from page 1

        try:
            while True:  # Loop through pagination until no more pages
                page_url = f"{self.config['base_url']}?page={page_number}" if page_number > 1 else self.config['base_url']
                self.logger.info(f"Scraping category page {page_number}: {page_url}")
                driver.get(page_url)
                time.sleep(5)  # Allow page to load

                # Scroll down to load all content
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)

                # Extract product links
                product_links = driver.find_elements(By.CSS_SELECTOR, "div.page--full-width.page--grid a")

                if not product_links:
                    self.logger.warning(f"No product links found on page {page_number}!")
                    break  # Stop if no products found (possibly last page)

                # Add valid product links to the queue
                for link_elem in product_links:
                    try:
                        product_url = link_elem.get_attribute('href')
                        product_name = link_elem.text.strip()

                        if "/products/" in product_url:
                            self.product_queue.put({'url': product_url, 'name': product_name})
                            self.logger.info(f"Found product: {product_name} ({product_url})")
                    except Exception as e:
                        self.logger.warning(f"Error processing product link: {str(e)}")

                # Check if a "Next" button exists to continue pagination
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "li.pagination-item--next a")
                    next_page_url = next_button.get_attribute("href")
                    if next_page_url:
                        page_number += 1  # Move to next page
                    else:
                        break  # No next page, stop
                except NoSuchElementException:
                    break  # No "Next" button found, stop

        except Exception as e:
            self.logger.error(f"Failed to scrape category pages: {str(e)}")
        finally:
            driver.quit()

    def extract_product_details(self, product_info):
        """Extract product details and save to a text file."""
        url = product_info['url']
        name = product_info['name']

        # Clean file name to avoid OS issues
        safe_filename = "".join(c if c.isalnum() or c in " _-" else "_" for c in name) + ".txt"
        file_path = os.path.join(self.config['output_dir'], safe_filename)

        driver = self.get_browser()
        try:
            driver.get(url)
            time.sleep(3)

            # Extract product name and details
            product_name = driver.find_element(By.TAG_NAME, "h1").text
            details_section = driver.find_element(By.CLASS_NAME, "productView-description").text

            with open(file_path, "w", encoding="utf-8") as file:
                file.write(f"{product_name}\n\n")
                file.write(details_section)

            self.logger.info(f"Saved: {file_path}")

        except Exception as e:
            self.logger.error(f"Error extracting {name}: {str(e)}")

        finally:
            driver.quit()

    def process_product_queue(self):
        """Process product queue using multiple threads."""
        total_products = self.product_queue.qsize()
        self.logger.info(f"Processing {total_products} products...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            while not self.product_queue.empty():
                product_info = self.product_queue.get()
                future = executor.submit(self.extract_product_details, product_info)
                futures.append(future)
                time.sleep(2.0)

    def scrape(self):
        """Run full scraping process."""
        self.logger.info("Starting scraping process...")
        self.scrape_category_pages()
        self.process_product_queue()
        self.logger.info("Scraping completed successfully!")

def main():
    scraper = NewDirectionsScraper()
    scraper.scrape()

if __name__ == "__main__":
    main()

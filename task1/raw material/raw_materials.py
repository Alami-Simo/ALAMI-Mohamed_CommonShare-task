import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

def export_natrue_data():
    # Setup Chrome options
    chrome_options = Options()
    # Keep the browser visible to debug (comment this out to run headless)
    # chrome_options.add_argument("--headless")
    
    # Set download directory to current working directory
    download_dir = os.path.abspath(os.getcwd())
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Initialize the webdriver with proper service
    try:
        # This will automatically download the correct ChromeDriver version
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Go directly to page 1
        url = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=raw-materials&prod[pageIndex]=1&prod[search]=&brands[pageNumber]=1&brands[filters][letter]="
        print(f"Navigating to: {url}")
        driver.get(url)
        
        # Wait for page to load completely
        time.sleep(5)
        
        # Using the exact CSS selector you provided
        print("Looking for export button using exact CSS selector...")
        css_selector = "#pane-raw-materials > div > div.mt-3 > div.w-25 > section.text-right > button.btn.btn-sm.btn-outline-success.px-3"
        
        # Wait for the button to be clickable
        export_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector))
        )
        
        print("Export button found, clicking...")
        
        # Try different methods to click the button
        try:
            # Method 1: Basic click
            export_button.click()
        except Exception as e:
            print(f"Basic click failed: {str(e)}")
            try:
                # Method 2: JavaScript click
                driver.execute_script("arguments[0].click();", export_button)
                print("Clicked using JavaScript")
            except Exception as e2:
                print(f"JavaScript click failed: {str(e2)}")
                try:
                    # Method 3: ActionChains click
                    ActionChains(driver).move_to_element(export_button).click().perform()
                    print("Clicked using ActionChains")
                except Exception as e3:
                    print(f"ActionChains click failed: {str(e3)}")
        
        # Wait for download to complete
        print("Waiting for download to complete...")
        time.sleep(15)
        
        print(f"Download should be complete. Check your downloads folder or {download_dir}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Keep the browser open for debugging
        input("Press Enter to close the browser...")
        if 'driver' in locals():
            driver.quit()

if __name__ == "__main__":
    export_natrue_data()
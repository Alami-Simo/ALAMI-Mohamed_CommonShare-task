from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

def extract_product_details(url, filename):
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    
    time.sleep(3)  # Wait for page to load completely
    
    try:
        product_name = driver.find_element(By.TAG_NAME, "h1").text
        
        # Extracting full product details as plain text
        details_section = driver.find_element(By.CLASS_NAME, "productView-description").text
        
        with open(filename, "w", encoding="utf-8") as file:
            file.write(f"{product_name}\n\n")
            file.write(details_section)
        
        print(f"Product details saved to {filename}")
        
    except Exception as e:
        print(f"Error extracting product details: {e}")
    
    driver.quit()


url = "https://www.newdirectionsaromatics.com/products/activated-coconut-charcoal-powder-raw-material"
filename = "product_details.txt"
extract_product_details(url, filename)
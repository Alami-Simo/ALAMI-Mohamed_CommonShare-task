import pytest
import os
import json
import pandas as pd
from unittest.mock import patch, mock_open, MagicMock
from Products import (
    initialize_files, get_processed_products, add_to_processed_products,
    append_to_json, product_exists_in_excel, append_to_excel,
    merge_temp_files_to_excel, setup_driver
)

# Test file paths
TEST_JSON_FILE = "natrue_product_details.json"
TEST_EXCEL_FILE = "natrue_product_details.xlsx"
TEST_TEMP_DIR = "temp_files"
TEST_PROCESSED_PRODUCTS_FILE = "processed_products.json"

@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown():
    """Setup and teardown fixture for test environment."""
    global JSON_FILE, EXCEL_FILE, TEMP_DIR, PROCESSED_PRODUCTS_FILE
    JSON_FILE = TEST_JSON_FILE
    EXCEL_FILE = TEST_EXCEL_FILE
    TEMP_DIR = TEST_TEMP_DIR
    PROCESSED_PRODUCTS_FILE = TEST_PROCESSED_PRODUCTS_FILE

    initialize_files()
    yield

    # Cleanup test files after tests
    for file in [TEST_JSON_FILE, TEST_EXCEL_FILE, TEST_PROCESSED_PRODUCTS_FILE]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(TEST_TEMP_DIR):
        for f in os.listdir(TEST_TEMP_DIR):
            os.remove(os.path.join(TEST_TEMP_DIR, f))
        os.rmdir(TEST_TEMP_DIR)

def test_initialize_files():
    """Test file and directory initialization."""
    assert os.path.exists(TEST_JSON_FILE)
    assert os.path.exists(TEST_EXCEL_FILE)
    assert os.path.exists(TEST_TEMP_DIR)
    assert os.path.exists(TEST_PROCESSED_PRODUCTS_FILE)

def test_get_processed_products():
    """Test fetching processed products."""
    with open(TEST_PROCESSED_PRODUCTS_FILE, "w", encoding="utf-8") as f:
        json.dump({"processed_products": ["Product A"]}, f)
    
    processed_products = get_processed_products()
    assert isinstance(processed_products, set)
    assert "Product A" in processed_products

def test_add_to_processed_products():
    """Test adding products to processed list."""
    add_to_processed_products("Product X")
    processed_products = get_processed_products()
    assert "Product X" in processed_products

def test_append_to_json():
    """Test appending a product to JSON file."""
    product_data = {
        "name": "Product A",
        "brand": "Brand X",
        "manufacturer": "Company X",
        "certification_level": "Certified",
        "certification_description": "Natural Product",
        "ingredients": "Water, Aloe Vera",
        "product_description": "Hydrating cream",
        "usage": "Apply daily",
        "image_url": "http://example.com/image.jpg",
        "page_number": 1
    }
    append_to_json(product_data)
    
    with open(TEST_JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    assert len(data["products"]) == 1
    assert data["products"][0]["name"] == "Product A"

def test_product_exists_in_excel():
    """Test checking if a product exists in Excel."""
    assert not product_exists_in_excel("Product A")  # Ensure product is not present initially
    
    product_data = {
        "name": "Product A",
        "brand": "Brand X",
        "manufacturer": "Company X",
        "certification_level": "Certified",
        "certification_description": "Natural Product",
        "ingredients": "Water, Aloe Vera",
        "product_description": "Hydrating cream",
        "usage": "Apply daily",
        "image_url": "http://example.com/image.jpg",
        "page_number": 1
    }
    
    append_to_excel(product_data)

    # 🔹 Ensure data is saved before checking
    merge_temp_files_to_excel()

    # Read the updated Excel file
    df = pd.read_excel(TEST_EXCEL_FILE)

    # 🔹 Case-insensitive check
    assert any(df["name"].str.lower() == "product a".lower())

def test_merge_temp_files_to_excel():
    """Test merging temp files into the main Excel file."""
    product_data = {
        "name": "Product Temp",
        "brand": "Brand Temp",
        "manufacturer": "Company Temp",
        "certification_level": "Certified",
        "certification_description": "Organic",
        "ingredients": "Olive Oil, Shea Butter",
        "product_description": "Moisturizing lotion",
        "usage": "Daily",
        "image_url": "http://example.com/temp.jpg",
        "page_number": 2
    }
    append_to_excel(product_data)
    merge_temp_files_to_excel()

    df_excel = pd.read_excel(TEST_EXCEL_FILE)
    assert "Product Temp" in df_excel["name"].values

def test_setup_driver():
    """Test Selenium WebDriver setup."""
    with patch("Products.webdriver.Chrome") as MockChrome:
        mock_driver = MagicMock()
        MockChrome.return_value = mock_driver
        driver = setup_driver()
        assert driver is not None
        driver.quit()

if __name__ == "__main__":
    pytest.main()

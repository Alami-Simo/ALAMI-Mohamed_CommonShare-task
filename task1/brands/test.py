import pytest
import os
import json
import pandas as pd
from brand import (
    initialize_files,
    get_processed_brands,
    add_to_processed_brands,
    append_to_json,
    brand_exists_in_excel,
    append_to_excel,
    merge_temp_files,
    get_total_pages
)

# Set up test files to avoid conflicts with actual data
TEST_JSON_FILE = "natrue_brand_details.json"
TEST_EXCEL_FILE = "natrue_brand_details.xlsx"
TEST_CSV_FILE = "natrue_brand_details.csv"
TEST_TEMP_DIR = "temp_brand_files"
TEST_PROCESSED_BRANDS_FILE = "processed_brands.json"

@pytest.fixture(scope="function", autouse=True)
def setup_and_teardown():
    """Setup test environment and teardown after tests."""
    global JSON_FILE, EXCEL_FILE, CSV_FILE, TEMP_DIR, PROCESSED_BRANDS_FILE
    JSON_FILE = TEST_JSON_FILE
    EXCEL_FILE = TEST_EXCEL_FILE
    CSV_FILE = TEST_CSV_FILE
    TEMP_DIR = TEST_TEMP_DIR
    PROCESSED_BRANDS_FILE = TEST_PROCESSED_BRANDS_FILE

    initialize_files()
    yield

    for file in [TEST_JSON_FILE, TEST_EXCEL_FILE, TEST_CSV_FILE, TEST_PROCESSED_BRANDS_FILE]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(TEST_TEMP_DIR):
        for f in os.listdir(TEST_TEMP_DIR):
            os.remove(os.path.join(TEST_TEMP_DIR, f))
        os.rmdir(TEST_TEMP_DIR)

def test_initialize_files():
    """Test if files and directories are initialized properly."""
    assert os.path.exists(TEST_JSON_FILE)
    assert os.path.exists(TEST_EXCEL_FILE)
    assert os.path.exists(TEST_CSV_FILE)
    assert os.path.exists(TEST_TEMP_DIR)
    assert os.path.exists(TEST_PROCESSED_BRANDS_FILE)

def test_get_processed_brands():
    """Test fetching processed brands."""
    processed_brands = get_processed_brands()
    assert isinstance(processed_brands, set)
    assert len(processed_brands) == 0  # Should be empty initially

def test_add_to_processed_brands():
    """Test adding brands to processed list."""
    add_to_processed_brands("Brand")
    processed_brands = get_processed_brands()
    assert "Brand" in processed_brands

def test_append_to_json():
    """Test appending a brand to JSON file."""
    brand_data = {
        "name": "Brand",
        "company": "Company",
        "address": "Address",
        "country": "Country",
        "website": "https://testbrand.com",
        "additional_info": "Info",
        "page_number": 1
    }
    append_to_json(brand_data)
    with open(TEST_JSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert len(data["brands"]) == 1
    assert data["brands"][0]["name"] == "Brand"

def test_brand_exists_in_excel():
    """Test checking if a brand exists in Excel."""
    assert not brand_exists_in_excel("Brand")  # Ensure brand is not present initially

    # Add brand and check again
    brand_data = {
        "name": "Brand",
        "company": "Company",
        "address": "Address",
        "country": "Country",
        "website": "https://testbrand.com",
        "additional_info": "Info",
        "page_number": 1
    }
    append_to_excel(brand_data)

    # Ensure data is saved before checking
    merge_temp_files()

    # Read Excel file to verify the entry exists
    df = pd.read_excel(TEST_EXCEL_FILE)

    # Check with case insensitivity
    assert any(df["name"].str.lower() == "brand".lower())

def test_merge_temp_files():
    """Test merging temp files into the main Excel and CSV."""
    brand_data = {
        "name": "Temp Brand",
        "company": "Temp Company",
        "address": "Temp Address",
        "country": "Temp Country",
        "website": "https://tempbrand.com",
        "additional_info": "Temp Info",
        "page_number": 2
    }
    append_to_excel(brand_data)
    merge_temp_files()

    df_excel = pd.read_excel(TEST_EXCEL_FILE)
    df_csv = pd.read_csv(TEST_CSV_FILE)

    assert "Temp Brand" in df_excel["name"].values
    assert "Temp Brand" in df_csv["name"].values

def test_get_total_pages(monkeypatch):
    """Mock test for get_total_pages() as it requires Selenium."""
    def mock_get_total_pages():
        return 12

    monkeypatch.setattr("brand.get_total_pages", mock_get_total_pages)
    assert get_total_pages() == 12

if __name__ == "__main__":
    pytest.main()

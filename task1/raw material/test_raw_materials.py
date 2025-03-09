import pytest
from unittest.mock import patch, MagicMock
import raw_materials

@pytest.fixture
def mock_driver():
    """Mock Selenium WebDriver."""
    with patch("raw_materials.webdriver.Chrome") as MockWebDriver:
        mock_driver = MagicMock()
        MockWebDriver.return_value = mock_driver
        yield mock_driver

def test_export_natrue_data(mock_driver):
    """Test if the export function navigates to the correct URL and clicks the export button."""
    
    # Mock necessary Selenium functions
    mock_driver.get = MagicMock()
    mock_driver.find_element = MagicMock()
    mock_driver.execute_script = MagicMock()

    with patch("raw_materials.WebDriverWait") as MockWait:
        # Mock WebDriverWait behavior
        mock_wait_instance = MagicMock()
        MockWait.return_value.until.return_value = mock_wait_instance
        mock_wait_instance.click = MagicMock()

        # Mock input() to prevent the test from waiting for user input
        with patch("builtins.input", return_value=""):
            with patch("time.sleep", return_value=None):  # Prevent real sleep
                raw_materials.export_natrue_data()

        # Verify that the correct URL was loaded
        expected_url = "https://natrue.org/our-standard/natrue-certified-world/?database[tab]=raw-materials&prod[pageIndex]=1&prod[search]=&brands[pageNumber]=1&brands[filters][letter]="
        mock_driver.get.assert_called_once_with(expected_url)

        # Verify the export button was clicked
        MockWait.return_value.until.assert_called()
        mock_wait_instance.click.assert_called()

def test_browser_closes_on_exit(mock_driver):
    """Test that the browser quits after function execution."""
    
    with patch("time.sleep", return_value=None):  # Avoid sleep delays
        with patch("builtins.input", return_value=""):  # Mock input to auto-close
            raw_materials.export_natrue_data()
    
    mock_driver.quit.assert_called_once()

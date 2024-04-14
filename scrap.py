from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def main():
    url = "https://8za.me/overall.htm"
    with webdriver.Chrome(service=Service(ChromeDriverManager().install())) as driver:
        driver.get(url)
        print("hello")


if __name__ == "__main__":
    main()
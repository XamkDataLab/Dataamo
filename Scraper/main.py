import os
from dotenv import load_dotenv
from components.datepicker import datepicker
from webdriver_manager.chrome import ChromeDriverManager
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By

#Load environment variables, os.environ includes OS user's env variables and the ones in .env file
#Can be accessed like: os.environ['env-variable-name']
load_dotenv()


#Browser driver initiation

#Chrome driver has a bug since 2015 that throws random errors so we'll add an option to clean up the log a bit
options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging'])
driver = webdriver.Chrome(options=options)

def menu():
    while True:
        service = input('''
Valitse palvelu:
    1. Mallioikeus -tietopalvelu
    2. Tavaramerkki -tietopalvelu
    3. Lopeta
''')

        date = datepicker()
        match int(service):
            case 1:
                navigator(os.environ['mallioikeus'], date)
            case 2:
                navigator(os.environ['tavaramerkki'], date)
            case 3:
                quit()

#URL as typical string, date in form of: {'start': 01.01.2023, 'end': 29.01.2023}
def navigator(url: str, date: dict):
    driver.get(url)
    startingDateField = driver.find_element(By.NAME, "applicationStartDate")
    endingDateField = driver.find_element(By.NAME, "applicationEndDate")
    #Since the site uses some weird custom element type, we'll locate the submit button with xpath
    submitButton = driver.find_element(By.XPATH, "//button[@data-cy='submit']")

    startingDateField.send_keys(date['start'])
    endingDateField.send_keys(date['end'])
    submitButton.click()
    

if __name__ == '__main__':
    menu()
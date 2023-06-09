import os
from dotenv import load_dotenv
from components.datepicker import datepicker
from components.filehandler import fileHandler
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#Load environment variables, os.environ includes OS user's env variables and the ones in .env file
#Can be accessed like: os.environ['env-variable-name']
load_dotenv()

if os.path.isdir(f'{os.path.dirname(os.path.realpath(__file__))}\downloads') == False:
    os.mkdir(f'{os.path.dirname(os.path.realpath(__file__))}\downloads')


#Browser initiation
downloadFolder = f'{os.path.dirname(os.path.realpath(__file__))}\downloads'
options = webdriver.ChromeOptions()
#This is to clean up the log a bit, chrome driver has a bug that throws random errors that don't mean or affect anything
options.add_experimental_option('excludeSwitches', ['enable-logging'])
#Add download folder to current directory for file parsing
options.add_experimental_option("prefs", {
  "download.default_directory": downloadFolder,
  "download.prompt_for_download": False,
  "download.directory_upgrade": True,
  "safebrowsing.enabled": True
})
driver = webdriver.Chrome(options=options)

def menu():
    global serviceName
    while True:
        try:
            service = int(input('''
Valitse palvelu:
    1. Mallioikeus -tietopalvelu
    2. Tavaramerkki -tietopalvelu
    3. Lopeta
'''))
            match service:
                case 1:
                    date = datepicker()
                    serviceName = "mallioikeus"
                    searchNavigator(os.environ['mallioikeus'], date)
                case 2:
                    date = datepicker()
                    serviceName = "tavaramerkki"
                    searchNavigator(os.environ['tavaramerkki'], date)
                case 3:
                    quit()
        except ValueError:
            print("\nValitse numeroina 1-3.")
        else:
            if service > 3 or service < 1:
                print("\nVäärä valinta, valitse 1-3.")

#URL as typical string, date in form of: {'start': 01.01.2023, 'end': 29.01.2023}
def searchNavigator(url: str, date: dict):
    driver.get(url)
    startingDateField = driver.find_element(By.NAME, "applicationStartDate")
    endingDateField = driver.find_element(By.NAME, "applicationEndDate")
    #Since the site uses some weird custom element type, we'll locate the submit button with xpath
    submitButton = driver.find_element(By.XPATH, "//button[@data-cy='submit']")

    startingDateField.send_keys(date['start'])
    endingDateField.send_keys(date['end'])
    submitButton.click()
    resultsNavigator(date)

#Date is included here for the file renaming
def resultsNavigator(date: dict):
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//button[starts-with(@class,'btn-icon')]"))) #Wait for load
    #Site is a react app, the button classname is dynamic and could change in future - search for download button with xpath
    downloadButton = driver.find_element(By.XPATH, "//button[starts-with(@class,'btn-icon')]")
    downloadButton.click()



    #Send file to be handled by filehandler component
    fileHandler(downloadFolder, date, serviceName)

if __name__ == '__main__':
    menu()
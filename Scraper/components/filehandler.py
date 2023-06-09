import os, time
import pandas as pd

def fileHandler(path: str, date: dict, prefix: str):
    #Wait for the download to finish
    downloadWaiter(path)
    #Read file
    file = pd.read_csv(f'{path}\\results.csv', sep='delimiter')
    #Insert to SQL
    print("FILE /components/filehandler.py, ROW 11 - INSERT THE SQL FUNCTION HERE WHEN NEEDED")
    #SQLinsert(file, prefix)
    #Rename the file
    renameFile(path, date, prefix)

#Rename the downloaded file to the date range which its the result of
#Technically this isn't important since saved files aren't used later for other than parsing, 
#but for future possible uses we'll archive them 
def renameFile(path: str, date: dict, prefix: str):
    downloadWaiter(path)
    oldFile = f'{path}\\results.csv'
    newFile = f'{path}\\{prefix}_{date["start"]}-{date["end"]}.csv'
    try:
        os.rename(oldFile, newFile)
        print("Tiedosto ladattu ja käsitelty - Valmis! \n")
    except OSError as E:
        if E.winerror == 183:
            i = 0
            for file in os.listdir(path):
                if f'{prefix}_{date["start"]}-{date["end"]}' in file:
                    i+=1
            print(f'Tiedosto on jo olemassa, luodaan tiedosto nimellä {prefix}_{date["start"]}-{date["end"]}({i}).csv')
            os.rename(oldFile, f'{path}\\{prefix}_{date["start"]}-{date["end"]}({i}).csv')
        else:
            print(f"Error: {E}")

#This program has a stroke if we don't wait for the file to be downloaded
def downloadWaiter(path):
    repeat = True
    while repeat:
        #Since the file will always be named results.csv, we'll keep scanning the folder until it appears
        for file in os.listdir(path):
            if file == "results.csv":
                repeat = False
        time.sleep(1)
        
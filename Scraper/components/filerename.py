import os, time

#Rename the downloaded file to the date range which its the result of
#Technically this isn't important since saved files aren't used later for other than parsing, 
#but for future possible uses we'll archive them 
def renameFile(path: str, date: dict):
    downloadWaiter(path)
    oldFile = f'{path}\\results.csv'
    newFile = f'{path}\\{date["start"]}-{date["end"]}.csv'
    os.rename(oldFile, newFile)

#This program has a stroke if we don't wait for the file to be downloaded
def downloadWaiter(path):
    repeat = True
    while repeat:
        #Since the file will always be named results.csv, we'll keep scanning the folder until it appears
        for file in os.listdir(path):
            if file == "results.csv":
                repeat = False
        time.sleep(1)
        
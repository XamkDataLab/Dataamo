import pyodbc
import os
from dotenv import load_dotenv
import sqlparse
import progressbar

def init_connection(_ENV = "local"):

    if _ENV == "local":
        load_dotenv(".env.local")
    elif _ENV == "live":
        load_dotenv(".env.live")

    DRIVER = os.getenv("DRIVER")
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE")
    UID = os.getenv("UID")
    PWD = os.getenv("PWD")

    connectionString = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD};'
    return pyodbc.connect(connectionString)

def execute_file(filename, connection):
    cursor = connection.cursor()
    chunk_size = 100*1024
    total_size = os.path.getsize(filename)
    progress = 0
    with open(filename, 'r', encoding='utf-8') as file:
        sql_commands = ''
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            sql_commands += chunk

            # Calculate and display progress
            progress += len(chunk)
            percentage = (progress / total_size) * 100
            print(f"Progress: {percentage:.2f}%")
            
        parsed = sqlparse.split(sql_commands)

        progress = progressbar.ProgressBar(max_value=len(parsed))
        progress.update(0)

        for index, statement in enumerate(parsed):
            if statement.strip():
                cursor.execute(statement)
            connection.commit()
            progress.update(index, force=True)

    progress.finish()
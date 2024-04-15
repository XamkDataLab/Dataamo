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

    return False

def execute_file(filename, cursor):
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
            
        # Use sqlparse to split the SQL commands
        parsed = sqlparse.split(sql_commands)

        # Create a progress bar widget
        progress = progressbar.ProgressBar(max_value=len(parsed))
        progress.update(0)

        for index, statement in enumerate(parsed):
            if statement.strip():
                cursor.execute(statement)
                    
            # Commit the transaction
            connection.commit()

            # Update the progress bar
            progress.update(index, force=True)

    # Finish the progress bar
    progress.finish()
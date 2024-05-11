import pyodbc
import os
from dotenv import load_dotenv

class DatabaseClient:
    def __init__(self, _ENV="local"):
        self.connection = self._init_connection(_ENV)

    def _init_connection(self, _ENV):
        if _ENV == "local":
            load_dotenv(".env.local")
        elif _ENV == "live":
            load_dotenv(".env.live")

        DRIVER = os.getenv("DRIVER")
        SERVER = os.getenv("SERVER")
        DATABASE = os.getenv("DATABASE")
        UID = os.getenv("UID")
        PWD = os.getenv("PWD")

        try:
            connectionString = f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD};'
            return pyodbc.connect(connectionString, timeout=10)
        except pyodbc.OperationalError as e:
            print("Connection to database timed out.")
            return None

# Example usage
#db_client = DatabaseClient()  # Creates the client and connects to the database

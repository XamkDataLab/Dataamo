import pyodbc
import os
from dotenv import load_dotenv

class DatabaseClient:
    def __init__(self, env="live"):
        self.env = env
        self.connection = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def _connect(self):
        if self.env == "local":
            load_dotenv(".env.local")
        elif self.env == "live":
            load_dotenv(".env.live")
        else:
            raise ValueError(f"Invalid environment: {self.env}")
        
        config = {
            'DRIVER': os.getenv("DRIVER"),
            'SERVER': os.getenv("SERVER"),
            'DATABASE': os.getenv("DATABASE"),
            'UID': os.getenv("UID"),
            'PWD': os.getenv("PWD"),  # Consider using secrets management instead
        }

        missing_config = [key for key, value in config.items() if not value]
        if missing_config:
            raise ValueError(f"Missing configuration values: {', '.join(missing_config)}")

        connection_string = ';'.join([f"{key}={value}" for key, value in config.items()])
        
        try:
            self.connection = pyodbc.connect(connection_string)
            self.connection.timeout = 10
        except pyodbc.OperationalError as e:
            raise ConnectionError(f"Could not connect to the database in {self.env} environment: {e}")
    

# Example usage
#with DatabaseClient() as db_client:  
#    cursor = db_client.connection.cursor()
#    cursor.execute("SELECT * FROM your_table")
#    rows = cursor.fetchall()
#
#    for row in rows:
#        print(row)

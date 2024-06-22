import pyodbc
import os
from dotenv import load_dotenv

class DatabaseError(Exception):
    """Custom exception for database-related errors."""
    pass

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
        """Connect to a database, either a local one or a live one."""

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

    def query(self, query, params=None):
        """Executes a SQL query and returns the results."""

        if not self.connection:
            raise ConnectionError("Not connected to the database.")

        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)  # Use parameterized query for security
            else:
                cursor.execute(query)

            # Fetch results based on query type
            if cursor.description:  # If the query returns rows (e.g., SELECT)
                columns = [column[0] for column in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            else:
                return cursor.rowcount  # Return number of rows affected (e.g., INSERT, UPDATE, DELETE)
                
        except pyodbc.Error as e:
            raise DatabaseError(f"Error executing query: {e}")
        finally:
            if cursor:
                cursor.close()
                
# Example usage
#with DatabaseClient() as db_client:  
#    cursor = db_client.connection.cursor()
#    cursor.execute("SELECT * FROM your_table")
#    rows = cursor.fetchall()
#
#    for row in rows:
#        print(row)

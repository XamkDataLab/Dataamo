import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

class DatabaseError(Exception):
    pass

class DatabaseClient:
    def __init__(self, env="live"):
        self.env = env
        self._load_dotenv()
        self._engine = self._create_engine()
        self._session_factory = scoped_session(sessionmaker(bind=self._engine))

    def __enter__(self):
        self._session = self._session_factory()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self._session.rollback()
        else:
            self._session.commit()
        self._session.close()

    def _load_dotenv(self):
        if self.env == "local":
            load_dotenv(".env.local")
        elif self.env == "live":
            load_dotenv(".env.live")
        else:
            raise ValueError(f"Invalid environment: {self.env}")

    def _create_engine(self):
        """
        Creates a SQLAlchemy engine based on the environment ('live' or 'local').
        Reads connection details from environment variables.
        """
        if self.env == "live":
            # Azure SQL connection for the 'live' environment.
            driver = os.getenv("DRIVER")
            server = os.getenv("SERVER")
            database = os.getenv("DATABASE")
            uid = os.getenv("UID")
            passwd = os.getenv("PWD")

            if not all([driver, server, database, uid, passwd]):
                raise ValueError("Missing Azure SQL configuration values.")
            
            # URL-encode the password to handle special characters
            passwd = quote_plus(passwd)
            connection_string = f"mssql+pyodbc://{uid}:{passwd}@{server}/{database}?driver={driver}"

        elif self.env == "local":
            # PostgreSQL connection for the 'local' environment.
            # Provides default values for some variables.
            server = os.getenv("SERVER")
            port = os.getenv("PORT")
            database = os.getenv("DATABASE")
            uid = os.getenv("UID")
            passwd = os.getenv("PWD")

            if not all([server, port, database, uid, passwd]):
                raise ValueError("Missing PostgreSQL configuration values.")

            connection_string = f"postgresql+psycopg2://{uid}:{passwd}@{server}:{port}/{database}"
            
        else:
            raise ValueError(f"Invalid environment: {self.env}. Must be 'live' or 'local'.")

        # Returns the engine with echo=False to avoid logging all SQL statements.
        return create_engine(connection_string, echo=False)  

    def query(self, query, params=None):
        try:
            result = self._session.execute(text(query), params)

            if result.returns_rows:
                rows = result.fetchall()                
                return rows
            else:
                return result.rowcount
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error executing query: {e}")

    def upsert(self, table_name, key_column, data):
        """
        Perform an upsert operation.

        :param table_name: Name of the table.
        :param key_column: The unique key column to check for existing records.
        :param data: Dictionary of column-value pairs to upsert. Must include the key column.
        """
        if key_column not in data:
            raise ValueError(f"Key column '{key_column}' must be in the data dictionary.")

        columns = list(data.keys())
        placeholders = ', '.join([f":{col}" for col in columns])
        update_columns = ', '.join([f"{col} = :{col}" for col in columns if col != key_column])
        
        update_sql = f"UPDATE {table_name} SET {update_columns} WHERE {key_column} = :{key_column}"
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        try:
            # Execute update
            update_result = self._session.execute(text(update_sql), data)
            
            # Check if any row was updated
            if update_result.rowcount == 0:
                # If no rows were updated, insert a new row
                self._session.execute(text(insert_sql), data)
            
            self._session.commit()
        
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error upserting into {table_name}: {e}")

    def insert_dataframe_to_table(self, df, table_name):
        try:
            df.to_sql(table_name, self._engine, if_exists='append', index=False)
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error inserting dataframe: {e}")

    def delete(self, table_name, key_column, key_value):
        """Deletes records from a table based on a key column and value."""
        with self._engine.connect() as connection:
            try:
                sql = f"DELETE FROM {table_name} WHERE {key_column} = :key_value"
                connection.execute(text(sql), {"key_value": key_value})
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise RuntimeError(f"Error deleting from {table_name}: {e}")

    def insert(self, table_name, data):
        """Inserts a single record into a table."""
        with self._engine.connect() as connection:
            try:
                columns = ", ".join(data.keys())
                placeholders = ", ".join([f":{key}" for key in data.keys()])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                connection.execute(text(sql), data)
                connection.commit()
            except Exception as e:
                connection.rollback()
                raise RuntimeError(f"Error inserting into {table_name}: {e}")

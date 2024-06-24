import os
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
        driver = os.getenv("DRIVER")
        server = os.getenv("SERVER")
        database = os.getenv("DATABASE")
        uid = os.getenv("UID")
        pwd = os.getenv("PWD") 

        if not all([driver, server, database, uid, pwd]):
            raise ValueError("Missing database configuration values.")

        connection_string = f"mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}"
        return create_engine(connection_string)  

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

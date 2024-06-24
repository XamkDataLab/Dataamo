import os
import logging
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

        # Ensure all necessary variables are set
        if not all([driver, server, database, uid, pwd]):
            raise ValueError("Missing database configuration values.")

        # Construct the SQLAlchemy-compatible connection string for ODBC
        connection_string = f"mssql+pyodbc://{uid}:{pwd}@{server}/{database}?driver={driver}"

        return create_engine(connection_string)  

    def query(self, query, params=None):
        try:
            result = self._session.execute(text(query), params)

            # Ensure that the query returns rows
            if result.returns_rows:
                rows = result.fetchall()                
                return rows
            else:
                return result.rowcount
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error executing query: {e}")


    def insert_dataframe_to_table(self, df, table_name):
        try:
            df.to_sql(table_name, self._engine, if_exists='append', index=False)
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error inserting dataframe: {e}")

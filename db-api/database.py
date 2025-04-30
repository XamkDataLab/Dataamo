import os
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from urllib.parse import quote_plus  # For URL-encoding the password

class DatabaseError(Exception):
    pass

class DatabaseClient:
    def __init__(self, env):
        self.env = env
        self._load_dotenv()
        self._engine = self._create_engine()
        self.dialect = self._engine.dialect.name
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
        # Validate environment first
        if self.env not in ["local", "live"]:
            raise ValueError(f"Invalid environment: {self.env}")

        # Construct the .env file name
        env_file_name = f".env.{self.env}"

        # Get the path of the current script file and its parent
        current_file_path = Path(__file__).resolve()
        env_dir = current_file_path.parent.parent

        # Construct the full .env file path
        env_file_path = env_dir / env_file_name

        if env_file_path.exists():
            load_dotenv(env_file_path)
        else:
            print(f"Environment file not found: {env_file_path}")
            raise ValueError(f"Environment file not found: {env_file_path}")

    def _create_engine(self):
        if self.env == "live":
            # Azure SQL connection
            driver = os.getenv("DRIVER")
            server = os.getenv("SERVER")
            database = os.getenv("DATABASE")
            uid = os.getenv("UID")
            passwd = quote_plus(os.getenv("PASSWD"))

            if not all([driver, server, database, uid, passwd]):
                raise ValueError("Missing Azure SQL configuration values.")

            connection_string = f"mssql+pyodbc://{uid}:{passwd}@{server}/{database}?driver={driver}"
        elif self.env == "local":
            # PostgreSQL connection
            host = os.getenv("SERVER", "localhost")
            port = os.getenv("PORT", "5432")
            database = os.getenv("DATABASE", "public")
            user = os.getenv("UID")
            password = os.getenv("PASSWD")

            if not all([host, port, database, user, password]):
                raise ValueError("Missing PostgreSQL configuration values.")

            connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        else:
            raise ValueError(f"Invalid environment: {self.env}")
        
        # For debugging, Enable SQLAlchemy echo
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

    def upsert(self, table_name, key_columns, data):
        """
        Perform an upsert operation with composite keys.

        :param table_name: Name of the table.
        :param key_columns: List of key column names.
        :param data: Dictionary of column-value pairs to upsert.
        """
        if not all(key_column in data for key_column in key_columns):
            raise ValueError(f"All key columns {key_columns} must be in the data dictionary.")

        columns = list(data.keys())
        placeholders = ', '.join([f":{col}" for col in columns])
        update_columns = ', '.join([f"{col} = :{col}" for col in columns if col not in key_columns])
        where_clause = ' AND '.join([f"{col} = :{col}" for col in key_columns])

        update_sql = f"UPDATE {table_name} SET {update_columns} WHERE {where_clause}"
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            update_result = self._session.execute(text(update_sql), data)

            if update_result.rowcount == 0:
                self._session.execute(text(insert_sql), data)

            self._session.commit()

        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error upserting into {table_name}: {e}")

    def insert(self, table_name, data):
        """
        Perform an insert operation.

        :param table_name: Name of the table.
        :param data: Dictionary of column-value pairs to insert. Must include all columns required by the table.
        """
        if not data:
            raise ValueError("Data dictionary cannot be empty.")

        columns = list(data.keys())
        placeholders = ', '.join([f":{col}" for col in columns])

        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            # Execute insert
            self._session.execute(text(insert_sql), data)
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error inserting into {table_name}: {e}")

    def delete(self, table_name, key_column, key_value):
        """
        Perform a delete operation.

        :param table_name: Name of the table.
        :param key_column: The unique key column to identify the record to delete.
        :param key_value: The value of the key column to delete the specific row.
        """
        if not key_column or key_value is None:
            raise ValueError(f"Key column '{key_column}' and key value must be provided.")

        delete_sql = f"DELETE FROM {table_name} WHERE {key_column} = :key_value"

        try:
            # Execute delete
            self._session.execute(text(delete_sql), {"key_value": key_value})
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error deleting from {table_name}: {e}")

    def exists(self, table_name, where, params):
        """
        Check if a record exists in the given table using a WHERE clause.

        Supports both PostgreSQL and Azure SQL.
        """
        named_params = {f"param{i}": val for i, val in enumerate(params)}
        where_clause = where
        for i in range(len(params)):
            where_clause = where_clause.replace("?", f":param{i}", 1)

        if self.dialect == "postgresql":
            sql = f"SELECT 1 FROM {table_name} WHERE {where_clause} LIMIT 1"
        elif self.dialect in ("mssql", "sql_server"):
            sql = f"SELECT TOP 1 1 FROM {table_name} WHERE {where_clause}"
        else:
            raise DatabaseError(f"Unsupported dialect: {self.dialect}")

        try:
            result = self._session.execute(text(sql), named_params)
            return result.first() is not None
        except SQLAlchemyError as e:
            raise DatabaseError(f"Error checking existence in {table_name}: {e}")

    def insert_dataframe_to_table(self, df, table_name):
        try:
            df.to_sql(table_name, self._engine, if_exists='append', index=False)
            self._session.commit()
        except SQLAlchemyError as e:
            self._session.rollback()
            raise DatabaseError(f"Error inserting dataframe: {e}")
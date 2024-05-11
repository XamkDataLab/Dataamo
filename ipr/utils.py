import progressbar
import sqlparse
import os

def execute_sql_file(db_client, filename, chunk_size=100 * 1024):
    """Executes a SQL file against the database using the provided DatabaseClient.

    Args:
        db_client: An instance of the DatabaseClient class.
        filename: The path to the SQL file.
        chunk_size: (Optional) The size of each chunk to read from the file (in bytes).
    """
    if not db_client.connection:
        print("Not connected to the database.")
        return

    cursor = db_client.connection.cursor()
    total_size = os.path.getsize(filename)
    read_size = 0
    sql_commands = ""

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
                try:
                    cursor.execute(statement)
                    db_client.connection.commit()
                except Exception as e:  # Use a broader Exception class to catch potential errors. 
                    print(f"Error executing SQL: {e}")
                    return
            progress.update(index, force=True)

    progress.finish()
    
    print("SQL file execution completed.")

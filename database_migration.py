import psycopg2
import subprocess
import os

# Define the parameters for the source and target databases
source_db_config = {
    'host': '',
    'port': '',
    'database': '',
    'user': '',
    'password': ''
}

target_db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'video_copy',  # This will be the new database name
    'user': '',
    'password': ''
}

# Step 1: Ensure the target database exists
try:
    con2 = psycopg2.connect(**{**target_db_config, 'database': 'postgres'})
    con2.autocommit = True
    with con2.cursor() as cur:
        cur.execute(f"CREATE DATABASE {target_db_config['database']}")  # This will fail if the database already exists
    con2.close()
    print("Target database created successfully.")
except psycopg2.errors.DuplicateDatabase:
    print("Target database already exists.")
except Exception as e:
    print(f"Error ensuring target database exists: {e}")

# Step 2: Dump the source database
try:
    dump_command = (
        f"pg_dump -h {source_db_config['host']} "
        f"-p {source_db_config['port']} "
        f"-U {source_db_config['user']} "
        f"-F c -b -v -f video.dump {source_db_config['database']}"
    )

    os.environ['PGPASSWORD'] = source_db_config['password']
    subprocess.run(dump_command, shell=True, check=True)
    print("Source database dumped successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error dumping source database: {e}")

# Step 3: Restore the dump to the target database with cleaning
try:
    restore_command = (
        f"pg_restore -h {target_db_config['host']} "
        f"-p {target_db_config['port']} "
        f"-U {target_db_config['user']} "
        f"-d {target_db_config['database']} -v --no-owner --clean --if-exists video.dump"
    )

    os.environ['PGPASSWORD'] = target_db_config['password']
    subprocess.run(restore_command, shell=True, check=True)
    print("Dump restored to target database successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error restoring dump to target database: {e}")

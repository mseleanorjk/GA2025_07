import sqlalchemy as sa
import pandas as pd
import shutil
from datetime import datetime
import gzip
import logging
import json

class HomeMessagesDB:
    """
    Class with methods handling the insertion, update, query and deletion of tables from a SQLite database

    Parameters:
        url: The URL pointing at the SQLite database
    """
    def __init__(self, url):
        self.url = url
        self.db = None
        
    def __repl__(self, url):
        return(f"The database has URL {self.url}")
        
    def create_db(self):
        """
        Create Database with (empty) tables if it doesn't exist; else connect to the database.
        Also, creates empty tables in the database in preparation for data insertion.

        Parameters:
        - self.url: the URL pointing at the database (initialised in self)
        """
        # Connecting to the db
        self.db = sa.create_engine(self.url)

        # Creating empty table smartthings
        with self.db.connect() as connection:
            try:
                create_query = sa.text("""CREATE TABLE IF NOT EXISTS smartthings (
                name TEXT NOT NULL,
                epoch TEXT NOT NULL,
                capability TEXT NOT NULL,
                attribute TEXT NOT NULL,
                unit TEXT,
                value_int NUMERIC,
                value_str TEXT
                )""")
                connection.execute(create_query)
            except Exception as e:
                logging.error(f"SQL CREATE function failed for table 'smartthings': {e}")
                raise e

            # Creating empty table devices
            try:
                devices_query = sa.text("""CREATE TABLE IF NOT EXISTS devices (
                name TEXT PRIMARY KEY,
                level TEXT NOT NULL,
                loc TEXT NOT NULL,
                FOREIGN KEY (name) 
                    REFERENCES smartthings (name)
                )""")
                connection.execute(devices_query)
            except Exception as e:
                logging.error(f"SQL CREATE function failed for table 'devices': {e}")
                raise e

            # Creating empty table p1e
            try:
                p1e_query = sa.text("""CREATE TABLE IF NOT EXISTS p1e (
                epoch INTEGER PRIMARY KEY,
                Import_T1_kWh NUMERIC,
                Import_T2_kWh NUMERIC,
                Export_T1_kWh NUMERIC,
                Export_T2_kWh NUMERIC,
                Electricity_imported_T1 NUMERIC,
                Electricity_imported_T2 NUMERIC,
                Electricity_exported_T1 NUMERIC,
                Electricity_exported_T2 NUMERIC,
                FOREIGN KEY (epoch) 
                    REFERENCES smartthings (epoch)
                )""")
                connection.execute(p1e_query)
            except Exception as e:
                logging.error(f"SQL CREATE function failed for table 'p1e': {e}")
                raise e

            # Creating empty table p1g
            try:
                p1g_query = sa.text("""CREATE TABLE IF NOT EXISTS p1g (
                epoch INTEGER PRIMARY KEY,
                Total_gas_used NUMERIC,
                FOREIGN KEY (epoch) 
                    REFERENCES smartthings (epoch)
                )""")
                connection.execute(p1g_query)
            except Exception as e:
                logging.error(f"SQL CREATE function failed for table 'p1g': {e}")
                raise e

            # Creating empty table for csv tracking
            try:
                tracking_query = sa.text("""CREATE TABLE IF NOT EXISTS tracking (
                file_name TEXT PRIMARY KEY
                )""")
                connection.execute(tracking_query)
            except Exception as e:
                logging.error(f"SQL CREATE function failed for table 'tracking': {e}")
                raise e

    def insert_table_smartthings(self,file_name):
        """
        Insert data from tsv files into the smartthings table

        Parameters:
        - self.db: The engine variable needed to start the connection
        - file_name: The name of the file containing the data to be inserted into the database
        """
        # Importing the data with Pandas
        smartthings = pd.read_csv(file_name, sep="\t")
        devices = pd.read_csv(file_name, sep="\t")

        # Preparing the data
        smartthings = smartthings.copy()
        smartthings["epoch"] = pd.to_datetime(smartthings["epoch"], utc=True).astype("int64") // 10**9  
        smartthings.loc[:, 'value_int'] = pd.to_numeric(smartthings['value'], errors='coerce')
        smartthings.loc[:, 'value_str'] = smartthings['value'].where(smartthings['value_int'].isna())
        smartthings.drop(["loc","level", "value"], inplace=True, axis = 1)

        # Inserting the table in the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.connect() as connection:
            result = connection.execute(check_query).fetchone()
            if result:
                logging.info(f"{file_name} was already appended to table 'smartthings'")
            else:
                try:
                    smartthings.to_sql("smartthings", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
        
        # Preparing the devices table, which contains information about the devices
        devices.drop(devices.columns.difference(["loc","level","name"]), axis=1, inplace=True)
        devices.drop_duplicates(inplace=True, ignore_index = True)

        # Inserting the devices table into the database
        with self.db.connect() as connection:
            for i in range(devices.shape[0]):
                insert_query = sa.text(f"""INSERT OR REPLACE INTO devices (name, level, loc) 
                    VALUES ('{devices.loc[i,"name"]}', '{devices.loc[i,"level"]}', '{devices.loc[i,"loc"]}')""")
                try:
                    connection.execute(insert_query)
                except Exception as e:
                    logging.error(f"Could not insert device {devices.loc[i,"name"]}: {e}")
                    raise e
        
    def insert_table_p1e(self, file_name):
        """
        Insert data from the csv files into the p1e table

        Parameters:
        - self.db: The enging variable needed to start the connection
        - file_name: The name of the file containing the data to be inserted into the database
        """
        # Importing the data
        p1e = pd.read_csv(file_name)

        # Preparing the data
        p1e["epoch"] = pd.to_datetime(p1e["time"], utc=True).astype("int64") // 10**9 
        p1e.drop("time", axis=1,inplace = True)
        
        for column in p1e:
            p1e.rename(columns = {column : column.replace(" ", "_")}, inplace = True)

        # Inserting the table into the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.connect() as connection:
            result = connection.execute(check_query).fetchone()
            if result:
                logging.info(f"{file_name} was already appended to table 'p1e'")
            else:
                try:
                    p1e.to_sql("p1e", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
        
    def insert_table_p1g(self, file_name):
        """
        Insert data from the csv files into the p1g table.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - file_name: The name of the file containing the data to be inserted into the database
        """
        # Importing the data
        p1g = pd.read_csv(file_name)

        # Preparing the data
        p1g["epoch"] = p1g["epoch"].timestamp()
        for column in p1g:
            p1g.rename(columns = {column : column.replace(" ", "_")}, inplace = True)

        # Inserting the table into the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.connect() as connection:
            result = connection.execute(check_query).fetchone()
            if result:
                logging.info(f"{file_name} was already appended to table 'p1g'")
            else:
                try:
                    p1g.to_sql("p1g", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e 

    def query_db(self, query):
        """
        Function handling queries to the database. 
        Input SQL code as string, returns a pandas dataframe with the query and allows saving query result as csv.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - query: The desired query to be carried out
        """
        # Querying and printing the result
        with self.db.connect() as connection:
            query = sa.text(query)
            try:
                result = connection.execute(query).fetchall()
            except Exception as e:
                logging.error(f"Could not execute the requested query: {e}")
                raise e
        try:
            df = pd.DataFrame(result)
        except Exception as e:
            logging.error(f"Could not convert the results of the query as a DataFrame: {e}")
            raise e

        # Option to save the result
        save_file = input("\nWould you like to save the result of this query as a new file? (y/N)\t")
        if save_file == "y":
            try:
                file_name = (f"query_result_{datetime.now()}").replace(" ","_").replace(":","_")
                df.to_csv(file_name)
                logging.info(f"File {file_name} saved successfully.")
            except Exception as e:
                logging.error(f"Could not save file {file_name}: {e}")
                raise e
        return(df)
    
    def drop_table(self, table_name):
        """
        Function handling table deletions. 
        Drops table from database and removes the corresponding file name from the 'tracking' table.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - table_name: The name of the table to be dropped
        """
        with self.db.connect() as connection:
            table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' and tbl_name = '{table_name}'")
            tables = connection.execute(table_names).fetchone()
            if tables:
                drop_query = sa.text(f"DROP TABLE {table_name}")
                connection.execute(drop_query)
                print("Table dropped successfully")
                delete_query = sa.text(f"DELETE FROM tracking WHERE file_name LIKE '%{table_name}%'")
            else:
                logging.error(f"Table {table_name} does not exist in the database {self.url}.")

from types import NoneType

import sqlalchemy as sa
import pandas as pd
import shutil
from datetime import datetime
import gzip
import logging
import click

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

    def create_smartthings_table(self):
        with self.db.connect() as connection:
            try:
                create_query = sa.text("""CREATE TABLE IF NOT EXISTS smartthings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                logging.error(f"SQL CREATE function failed for table 'smartthings':{e}")
                raise e
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

    def create_p1e_table(self):
        with self.db.begin() as connection:
                table_names = sa.text("SELECT name FROM sqlite_master WHERE type='table' and tbl_name = 'smartthings'")
                tables = connection.execute(table_names).fetchone()
                if tables:
                    try:
                        P1e_query = sa.text("""CREATE TABLE IF NOT EXISTS P1e (
                        epoch INTEGER PRIMARY KEY,
                        Electricity_imported_T1 NUMERIC,
                        Electricity_imported_T2 NUMERIC,
                        Electricity_exported_T1 NUMERIC,
                        Electricity_exported_T2 NUMERIC,
                        FOREIGN KEY (epoch) 
                            REFERENCES smartthings (epoch)
                        )""")
                        connection.execute(P1e_query)
                    except Exception as e:
                        logging.error(f"SQL CREATE function failed for table 'P1e': {e}")
                        raise e
                else:
                    try:
                        P1e_query = sa.text("""CREATE TABLE IF NOT EXISTS P1e (
                        epoch INTEGER PRIMARY KEY,
                        Electricity_imported_T1 NUMERIC,
                        Electricity_imported_T2 NUMERIC,
                        Electricity_exported_T1 NUMERIC,
                        Electricity_exported_T2 NUMERIC
                        )""")
                        connection.execute(P1e_query)
                    except Exception as e:
                        logging.error(f"SQL CREATE function failed for table 'P1e': {e}")
                        raise e

    def create_p1g_table(self):
        with self.db.begin() as connection:
                table_names = sa.text("SELECT name FROM sqlite_master WHERE type='table' and tbl_name = 'smartthings'")
                tables = connection.execute(table_names).fetchone()
                if tables:
                    try:
                        P1g_query = sa.text("""CREATE TABLE IF NOT EXISTS P1g (
                        epoch INTEGER PRIMARY KEY,
                        Total_gas_used NUMERIC,
                        FOREIGN KEY (epoch) 
                            REFERENCES smartthings (epoch)
                        )""")
                        connection.execute(P1g_query)
                    except Exception as e:
                        logging.error(f"SQL CREATE function failed for table 'P1g': {e}")
                        raise e
                else:
                    try:
                        P1g_query = sa.text("""CREATE TABLE IF NOT EXISTS P1g (
                        epoch INTEGER PRIMARY KEY,
                        Total_gas_used NUMERIC
                        )""")
                        connection.execute(P1g_query)
                    except Exception as e:
                        logging.error(f"SQL CREATE function failed for table 'P1g': {e}")
                        raise e
        
    def create_db(self):
        """
        Create Database with (empty) tables if it doesn't exist; else connect to the database.
        Also, creates empty tables in the database in preparation for data insertion.

        Parameters:
        - self.url: the URL pointing at the database (initialised in self)
        """
        # Connecting to the db
        self.db = sa.create_engine(self.url)

        # Creating empty tables
        self.create_smartthings_table()
        self.create_p1e_table()
        self.create_p1g_table()

        # Create tracking table
        with self.db.begin() as connection:
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

        # Create table if it was dropped
        self.create_smartthings_table()

        # Inserting the table in the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.begin() as connection:
            result = connection.execute(check_query).fetchone()
            if result:
                logging.info(f"{file_name} was already appended to table 'smartthings'")
            else:
                add_file_query = sa.text(f"INSERT INTO tracking (file_name) VALUES ('{file_name}')")
                connection.execute(add_file_query)
                try:
                    smartthings.to_sql("smartthings", connection, if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Could not insert data {file_name} in the smartthings table in the database {self.url}: {e}")
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
                    logging.error(f'Could not insert device {devices.loc[i,"name"]}: {e}')
                    raise e
        
    def insert_table_P1e(self, file_name):
        """
        Insert data from the csv files into the P1e table

        Parameters:
        - self.db: The engine variable needed to start the connection
        - file_name: The name of the file containing the data to be inserted into the database
        """
        # Importing the data
        P1e = pd.read_csv(file_name)

        # Preparing the data
        P1e["epoch"] = pd.to_datetime(P1e["time"], utc=True).astype("int64") // 10**9 
        P1e.drop("time", axis=1,inplace = True)
        P1e.columns = ['Electricity_imported_T1','Electricity_imported_T2','Electricity_exported_T1','Electricity_exported_T2','epoch']
        
        P1e.dropna(inplace=True, how= 'all', subset=[
                        'Electricity_imported_T1',
                        'Electricity_imported_T2',
                        'Electricity_exported_T1',
                        'Electricity_exported_T2'])

        # Create table if it was dropped
        self.create_p1e_table()
        
        # Temporary table for aggregation purposes
        with self.db.begin() as connection:
            P1e.to_sql("temp", connection, if_exists="replace", index=False)
            agg_query = sa.text("""SELECT epoch, 
                    avg(Electricity_imported_T1) as Electricity_imported_T1,
                    avg(Electricity_imported_T2) as Electricity_imported_T2,
                    avg(Electricity_exported_T1) as Electricity_exported_T1,
                    avg(Electricity_exported_T2) as Electricity_exported_T2
                    FROM (
                        SELECT epoch, 
                               Electricity_imported_T1, 
                               Electricity_imported_T2, 
                               Electricity_exported_T1, 
                               Electricity_exported_T2
                        FROM temp
                        UNION ALL
                        SELECT epoch, 
                               Electricity_imported_T1, 
                               Electricity_imported_T2, 
                               Electricity_exported_T1, 
                               Electricity_exported_T2
                        FROM P1e
                    )
                    GROUP BY epoch""")
            P1e_new = pd.read_sql(agg_query, con = connection)
        self.drop_table("temp")

        # Inserting the table into the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.begin() as connection:
           result = connection.execute(check_query).fetchone()
           if type(result) != NoneType:
                click.echo(f"{file_name} was already appended to table 'P1e'")
                logging.info(f"{file_name} was already appended to table 'P1e'")
           else:
                try:
                    P1e_new.to_sql("P1e", self.db.connect(), if_exists="replace", index=False)
                    add_file_query = sa.text(f"INSERT INTO tracking (file_name) VALUES ('{file_name}')")
                    connection.execute(add_file_query)
                except Exception as e:
                    logging.error(f"Could not insert data {file_name} in the P1e table in the database {self.url}: {e}")
                    raise e
        
    def insert_table_P1g(self, file_name):
        """
        Insert data from the csv files into the P1g table.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - file_name: The name of the file containing the data to be inserted into the database
        """
        # Importing the data
        P1g = pd.read_csv(file_name)

        # Preparing the data
        P1g["epoch"] = pd.to_datetime(P1g["time"], utc=True).astype("int64") // 10**9 
        P1g.drop("time", axis=1,inplace = True)
        for column in P1g:
            P1g.rename(columns = {column : column.replace(" ", "_")}, inplace = True)
        P1g.columns = ["Total_gas_used", "epoch"]
        cols = P1g.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        P1g.dropna(inplace=True)

        # Create table if it was dropped
        self.create_p1g_table()
        
        # Temporary table for aggregation purposes
        with self.db.begin() as connection:
            P1g.to_sql("temp", connection, if_exists="replace", index=False)
            agg_query = sa.text("""SELECT epoch, 
                        avg(Total_gas_used) as Total_gas_used
                        FROM (
                            SELECT *
                            FROM temp
                            UNION ALL
                            SELECT *
                            FROM P1g
                        )
                        GROUP BY epoch""")
            P1g_new = pd.read_sql(agg_query, con = connection)
        self.drop_table("temp")

        # Inserting the table into the database
        check_query = sa.text(f"SELECT file_name FROM tracking WHERE file_name='{file_name}'")
        with self.db.begin() as connection:
            result = connection.execute(check_query).fetchone()
            if result:
                logging.info(f"{file_name} was already appended to table 'P1g'")
            else:
                try:
                    P1g_new.to_sql("P1g", self.db.connect(), if_exists="append", index=False)
                    add_file_query = sa.text(f"INSERT INTO tracking (file_name) VALUES ('{file_name}')")
                    connection.execute(add_file_query)
                except Exception as e:
                    logging.error(f"Could not insert data {file_name} in the P1g table in the database {self.url}: {e}")
                    raise e 

    def query_db(self, query, save_file = False):
        """
        Function handling queries to the database. 
        Input SQL code as string, returns a pandas dataframe with the query and allows saving query result as csv.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - query: The desired query to be carried out
        - save_file: Option to save the file as a CSV, default to False
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
        if save_file:
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
        Function handling table dropping. 
        Drops table from database and removes the corresponding file name from the 'tracking' table.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - table_name: The name of the table to be dropped
        """
        if table_name == 'tracking':
            logging.error("The table 'tracking cannot be dropped")
        else:
            with self.db.begin() as connection:
                table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' and tbl_name = '{table_name}'")
                tables = connection.execute(table_names).fetchone()
                if tables:
                    drop_query = sa.text(f"DROP TABLE {table_name}")
                    connection.execute(drop_query)
                    print("Table dropped successfully")
                    delete_query = sa.text(f"DELETE FROM tracking WHERE file_name LIKE '%{table_name}%'")
                    connection.execute(delete_query)
                else:
                    logging.error(f"Table {table_name} does not exist in the database {self.url}.")

    def erase_table_content(self, table_name):
        """
        Function handling table deletions. 
        Deletes the data from the table and removes the corresponding file name from the 'tracking' table.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - table_name: The name of the table to delete the data from
        """
        with self.db.begin() as connection:
            table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' and tbl_name = '{table_name}'")
            tables = connection.execute(table_names).fetchone()
            if tables:
                try:
                    erase_query = sa.text(f"DELETE FROM {table_name} WHERE 1=1")
                    connection.execute(erase_query)
                    delete_query = sa.text(f"DELETE FROM tracking WHERE file_name LIKE '%{table_name}%'")
                    connection.execute(delete_query)
                    logging.info(f"Data in table {table_name} deleted successfully")
                except Exception as e:
                    logging.error(f"Could not erase the content of table {table_name}: {e}")
                    raise e
            else:
                logging.error(f"Table {table_name} does not exist in the database {self.url}.")

import sqlalchemy as sa
import pandas as pd
import shutil
from datetime import datetime
import gzip
import logging

class HomeMessagesDB:
    """
    Class with methods handling the insertion, update, query and deletion of tables from a SQLite database
    """
    def __init__(self, url):
        self.url = url
        
    def __repl__(self, url):
        return(f"The database has URL {self.url}")
        
    def create_db(self):
        """
        Create Database if it doesn't exist
        """
        db = sa.create_engine(self.url)
            
    def insert_table_smartthings(file_name):
        """
        Create (if it doesnt exist) tables of smart things and devices
        """

        # Importing the data with Pandas
        smartthings, devices = pd.read_csv(file_name, sep="\t")

        # Preparing the data
        smartthings["epoch"] = smartthings["epoch"].timestamp()
        smartthings = smartthings.copy()
        smartthings.loc[:, 'value_int'] = pd.to_numeric(smartthings['value'], errors='coerce')
        smartthings.loc[:, 'value_str'] = df['value'].where(smartthings['value_int'].isna())

        # Inserting the table in the database
        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS smartthings (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    epoch TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    attribute TEXT NOT NULL,
                    unit TEXT,
                    value_int NUMERIC,
                    value_str TEXT
                    )""")
                except Exception as e:
                    logging.error(f"SQL CREATE function failed: {e}")
                    raise e
                try:
                    pd.to_sql(smartthings, db.connect(), if_exist="append", index=True, index_label="id")
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e

        # Creating foreign keys only for the tables that exist
        for table in ["p1e","p1g"]:
            table_names = sa.text(f"SELECT tableName FROM sqlite_master WHERE type='table' AND tableName='{table}'")
            with db.connect() as connection:
                tables = connection.execute(table_names).fetchall()
                if table in tables:
                    connection.execute(f'''ALTER TABLE smartthings
                            ADD CONSTRAINT fk_smartthings_{table}
                            FOREIGN KEY (epoch) 
                                REFERENCES {table} (epoch)''')
        
        # Preparing the devices table, which contains information about the devices
        devices.drop(df.columns.difference(["loc","level","name"]), axis=1, inplace=True)
        devices.drop_duplicates(inplace=True)

        # Inserting the devices table into the database
        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS devices (
                    name TEXT PRIMARY KEY,
                    level TEXT NOT NULL,
                    loc TEXT NOT NULL,
                    FOREIGN KEY (name) 
                        REFERENCES smartthings (name)
                    )""")
                except Exception as e:
                    logging.error(f"SQL CREATE function failed: {e}")
                    raise e
                try:
                    pd.to_sql(devices, db.connect(), if_exist="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
        
    def insert_table_p1e(self, file_name):
        """
        Create p1e table if it does not exist
        """
        # Importing the data
        p1e = pd.read_csv(file_name)

        # Preparing the data
        p1e["epoch"] = p1e["epoch"].timestamp()
        for column in p1e:
            p1e.rename(columns = {column : column.replace(" ", "_")}, inplace = True)

        # Inserting the table into the database
        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS p1e (
                    epoch INTEGER PRIMARY KEY,
                    Import_T1_kWh NUMERIC,
                    Import_T2_kWh NUMERIC,
                    Export_T1_kWh NUMERIC,
                    Export_T2_kWh NUMERIC,
                    Electricity_imported_T1 NUMERIC,
                    Electricity_imported_T2 NUMERIC,
                    Electricity_exported_T1 NUMERIC,
                    Electricity_exported_T2 NUMERIC,
                    )""")
                except Exception as e:
                    logging.error(f"SQL CREATE function failed: {e}")
                    raise e
                try:
                    pd.to_sql(p1e, db.connect(), if_exist="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e

        # Handling the foreign key
        table_names = sa.text("SELECT tableName FROM sqlite_master WHERE type='table' AND tableName='smartthings'")
        with db.connect() as connection:
            tables = connection.execute(table_names).fetchall()
            if "smartthings" in tables:
                connection.execute('''ALTER TABLE p1e
                        ADD CONSTRAINT fk_p1e_smartthings
                        FOREIGN KEY (epoch) 
                        REFERENCES smartthings (epoch)''')
        
    def insert_table_p1g(self, file_name):
        """
        Create p1g table if it does not exist
        """
        # Importing the data
        p1g = pd.read_csv(file_name)

        # Preparing the data
        p1g["epoch"] = p1g["epoch"].timestamp()
        for column in p1g:
            p1g.rename(columns = {column : column.replace(" ", "_")}, inplace = True)

        # Inserting the table into the database
        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS p1g (
                    epoch INTEGER PRIMARY KEY,
                    Total_gas_used NUMERIC,
                )""")
                except Exception as e:
                    logging.error(f"SQL CREATE function failed: {e}")
                    raise e
                try:
                    pd.to_sql(p1g, db.connect(), if_exist="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                raise e   

        # Handling the foreign key
        table_names = sa.text("SELECT tableName FROM sqlite_master WHERE type='table' AND tableName='smartthings'")
        with db.connect() as connection:
            tables = connection.execute(table_names).fetchall()
            if "smartthings" in tables:
                connection.execute('''ALTER TABLE p1g
                        ADD CONSTRAINT fk_p1g_smartthings
                        FOREIGN KEY (epoch) 
                        REFERENCES smartthings (epoch)''')

    def query_db(self, query):
        """
        Function handling queries to the database
        """
        # Querying and printing the result
        with db.connect() as connection:
            result = connection.execute(query).fetchall()
        print(result)

        # Option to save the result
        save_file = input("\nWould you like to save the result of this query as a new file? (y/N)\t")
        if save_file == "y":
            result.to_csv(f"query_result_{datetime.now()}")

    def drop_table(self, table_name):
        """
        Function handling table deletions
        """
        table_names = sa.text("SELECT tableName FROM sqlite_master WHERE type='table' AND tableName='smartthings'")
        with db.connect() as connection:
            tables = connection.execute(table_names).fetchall()
            if table_name in tables:
                connection.execute(F'''DROP TABLE {table_name}''')
            else:
                logging.error(f"Table {table_name} does not exist in the database {self.url}.")
        
        
# if is_zipfile(file_name):
#            try:
#                shutil.unpack_archive(file_name)
#            except:
#                raise Exception as e:
#                    print(f"Could not unzip the file {file_name}: {e}")
#            file_name = file_name.replace(".zip", "")
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
        self.db = None
        
    def __repl__(self, url):
        return(f"The database has URL {self.url}")
        
    def create_db(self):
        """
        Create Database if it doesn't exist
        """
        self.db = sa.create_engine(self.url)
            
    def insert_table_smartthings(self,file_name):
        """
        Create (if it doesnt exist) tables of smart things and devices
        """

        # Importing the data with Pandas
        smartthings = pd.read_csv(file_name, sep="\t")
        devices = pd.read_csv(file_name, sep="\t")

        # Preparing the data
        
        # Change epoch to UNIX time  
    
        # Make a copy to avoid warnings
        smartthings = smartthings.copy()

        # Convert to datetime and then to UNIX timestamp 
        smartthings["epoch"] = pd.to_datetime(smartthings["epoch"], utc=True).astype("int64") // 10**9  
    
        smartthings.loc[:, 'value_int'] = pd.to_numeric(smartthings['value'], errors='coerce')
        smartthings.loc[:, 'value_str'] = smartthings['value'].where(smartthings['value_int'].isna())
        
        # Drop columns that won't be used here, but in the devices table
        smartthings.drop(["loc","level", "value"], inplace=True, axis = 1)

        # Inserting the table in the database
        with self.db.connect() as connection:
            try:
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
                    logging.error(f"SQL CREATE function failed for table {file_name}: {e}")
                    raise e
                try:
                    smartthings.to_sql("smartthings", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e

        # Creating foreign keys only for the tables that exist
        for table in ["p1e","p1g"]:
            table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            with self.db.connect() as connection:
                tables = connection.execute(table_names).fetchone()
                if tables:
                    fk_smartthings = sa.text(f'''ALTER TABLE smartthings
                            ADD CONSTRAINT fk_smartthings_{table}
                            FOREIGN KEY (epoch) 
                                REFERENCES {table} (epoch)''')
                    try:
                        connection.execute(fk_smartthings)
                        logging.info(f"Foreign key to table {table} created successfully.")
                    except Exception as e:
                        logging.error(f"Could not create foreign key to table {table}: {e}")
                        raise e
                else:
                    logging.info(f"Table {table} was not found.")
        
        # Preparing the devices table, which contains information about the devices
        devices.drop(devices.columns.difference(["loc","level","name"]), axis=1, inplace=True)
        devices.drop_duplicates(inplace=True)

        # Inserting the devices table into the database
        with self.db.connect() as connection:
            try:
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
                    logging.error(f"SQL CREATE function failed for table {file_name}: {e}")
                    raise e
                for i in range(devices.shape[0]):
                    insert_query = sa.text(f"""INSERT OR REPLACE INTO devices (name, level, loc) 
                        VALUES ({devices.loc[i,"name"]}, {devices.loc[i,"level"]}, {devices.loc[i,"loc"]})""")
                    try:
                        connection.execute(insert_query)
                    except Exception as e:
                        logging.error(f"Could not insert device {devices.loc[i,"name"]}: {e}")
                        raise e
#                try:
#                    devices.to_sql("devices", self.db.connect(), if_exist="append", index=False)
#                except Exception as e:
#                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
#                    raise e
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
        with self.db.connect() as connection:
            try:
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
                    )""")
                    connection.execute(p1e_query)
                except Exception as e:
                    logging.error(f"SQL CREATE function failed for table {file_name}: {e}")
                    raise e
                try:
                    p1e.to_sql("p1e", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                    logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                    raise e

        # Handling the foreign key
        table_names = sa.text("SELECT name FROM sqlite_master WHERE type='table' AND name='smartthings'")
        with self.db.connect() as connection:
            tables = connection.execute(table_names).fetchone()
            if tables:
                fk_p1e = sa.text('''ALTER TABLE p1e
                        ADD CONSTRAINT fk_p1e_smartthings
                        FOREIGN KEY (epoch) 
                        REFERENCES smartthings (epoch)''')
                try:
                    connection.execute(fk_p1e)
                    logging.info("Foreign key to table 'smartthings' created successfully")
                except Exception as e:
                    logging.error(f"Could not create foreign key to table 'smartthings': {e}")
                    raise e
            else:
                logging.info("Table 'smartthings' does not exist")
        
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
        with self.db.connect() as connection:
            try:
                try:
                    p1g_query = sa.text("""CREATE TABLE IF NOT EXISTS p1g (
                    epoch INTEGER PRIMARY KEY,
                    Total_gas_used NUMERIC,
                )""")
                    connection.execute(p1g_query)
                except Exception as e:
                    logging.error(f"SQL CREATE function failed for table {file_name}: {e}")
                    raise e
                try:
                    p1g.to_sql("p1g", self.db.connect(), if_exists="append", index=False)
                except Exception as e:
                    logging.error(f"Pandas could not insert table {file_name} in the database {self.url}: {e}")
                    raise e
            except Exception as e:
                logging.error(f"Could not insert table {file_name} in the database {self.url}: {e}")
                raise e   

        # Handling the foreign key
        table_names = sa.text("SELECT name FROM sqlite_master WHERE type='table' AND name='smartthings'")
        with self.db.connect() as connection:
            tables = connection.execute(table_names).fetchone()
            if tables:
                fk_p1g = sa.text('''ALTER TABLE p1g
                        ADD CONSTRAINT fk_p1g_smartthings
                        FOREIGN KEY (epoch) 
                        REFERENCES smartthings (epoch)''')
                try:
                    connection.execute(fk_p1g)
                    logging.info("Foreign key to table 'smartthings' created successfully")
                except Exception as e:
                    logging.error(f"Could not create foreign key to 'smartthings': {e}")
                    raise e
            else:
                logging.info("Table 'smartthings' does not exist.")

    def query_db(self, query):
        """
        Function handling queries to the database
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
                file_name = f"query_result_{datetime.now()}"
                df.to_csv(file_name.replace(" ", "_"))
                logging.info(f"File {file_name.replace(" ", "_")} saved successfully.")
            except Exception as e:
                logging.error(f"Could not save file {file_name.replace(" ", "_")}: {e}")
                raise e
        return(df)
    
    def drop_table(self, table_name):
        """
        Function handling table deletions
        """
        with self.db.connect() as connection:
            table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' and tbl_name = '{table_name}'")
            tables = connection.execute(table_names).fetchone()
            if table_name:
                drop_query = sa.text(f"DROP TABLE {table_name}")
                connection.execute(drop_query)
                print("Table dropped successfully")
            else:
                logging.error(f"Table {table_name} does not exist in the database {self.url}.")

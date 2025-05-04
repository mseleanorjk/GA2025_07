import sqlalchemy as sa
import pandas as pd
import shutil
from datetime import datetime
import gzip

class HomeMessagesDB:
    def __init__(self, url):
        self.url = url
        
    def __repl__(self, url):
        return(f"The database has URL {self.url}")
        
    def create_db():
        """
        Create Database if it doesn't exist
        """
        db = create_engine(self.url)
            
    def insert_table_smartthings(file_name):
        """
        Create (if it doesnt exist) tables of smart things and devices
        """

        smartthings, devices = pd.read_csv(file_name, sep="\t")
        
        smartthings["epoch"] = smartthings["epoch"].timestamp()
        numeric_values = pd.to_numeric(smartthings["value"], errors="coerce")
        smartthings["value_int"] = numeric_values
        smartthings["value_str"] = smartthings["value"].where(numeric_values.isna())
        smartthings.drop(["level","loc","value"], axis=1, inplace=True)
        
        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS smartthings (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    epoch TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    attribute TEXT NOT NULL,
                    value_int NUMERIC,
                    value_str TEXT,
                    unit TEXT,
                    FOREIGN KEY (name) 
                        REFERENCES devices (name),
                    FOREIGN KEY (epoch)
                        REFERENCES p1e (epoch),
                    FOREIGN KEY (epoch)
                        REFERENCES p1g (epoch)
                    )""")
                except:
                    raise Exception as e:
                        print(f"SQL CREATE function failed: {e}")
                try:
                    pd.to_sql(smartthings, db.connect(), if_exist="append", index=True, index_label="id")
                except:
                    raise Exception as e:
                        print(f"Pandas could not insert table {file_name} in the database: {e}")
            except:
                raise Exception as e:
                    print(f"Could not insert table {file_name} into the database: {e}")
        
        devices.drop(df.columns.difference(["loc","level","name"]), axis=1, inplace=True)
        devices.drop_duplicates(inplace=True)

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
                except:
                    raise Exception as e:
                        print(f"SQL CREATE function failed: {e}")
                try:
                    pd.to_sql(devices, db.connect(), if_exist="append", index=False)
                except:
                    raise Exception as e:
                        print(f"Pandas could not insert table {file_name} in the database: {e}")
            except:
                raise Exception as e:
                    print(f"Could not insert table {file_name} into the database: {e}")
        
    def insert_table_p1e(file_name):
        """
        Create p1e table if it does not exist
        """
        p1e = pd.read_csv(file_name)
        p1e["epoch"] = p1e["epoch"].timestamp()

        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS p1e (
                    epoch INTEGER PRIMARY KEY,
                    ImportT1kWh NUMERIC,
                    ImportT2kWh NUMERIC,
                    ExportT1kWh NUMERIC,
                    ExportT2kWh NUMERIC,
                    )""")
                except:
                    raise Exception as e:
                        print(f"SQL CREATE function failed: {e}")
                try:
                    pd.to_sql(p1e, db.connect(), if_exist="append", index=False)
                except:
                    raise Exception as e:
                        print(f"Pandas could not insert table {file_name} in the database: {e}")
            except:
                raise Exception as e:
                    print(f"Could not insert table {file_name} into the database: {e}")
        
    def insert_table_p1g(file_name):
        """
        Create p1g table if it does not exist
        """
        p1g = pd.read_csv(file_name)
        p1g["epoch"] = p1g["epoch"].timestamp()

        with db.connect() as connection:
            try:
                try:
                    connection.execute("""CREATE TABLE IF NOT EXISTS p1g (
                    epoch INTEGER PRIMARY KEY,
                    Total_gas_used NUMERIC,
                )""")
                except:
                    raise Exception as e:
                        print(f"SQL CREATE function failed: {e}")
                try:
                    pd.to_sql(p1g, db.connect(), if_exist="append", index=False)
                except:
                    raise Exception as e:
                        print(f"Pandas could not insert table {file_name} in the database: {e}")
            except:
                raise Exception as e:
                    print(f"Could not insert table {file_name} into the database: {e}")
        %%sql
        
    
        
# if is_zipfile(file_name):
#            try:
#                shutil.unpack_archive(file_name)
#            except:
#                raise Exception as e:
#                    print(f"Could not unzip the file {file_name}: {e}")
#            file_name = file_name.replace(".zip", "")
from types import NoneType

import sqlalchemy as sa
import pandas as pd
import shutil
from datetime import datetime
import gzip
import logging
import click
import os
import glob
from zoneinfo import ZoneInfo




def parse_user_answer(input):
    """
    Parses user answer to yes or no questions. Raises error if input cannot be understood. 

    Parameters:
    input: str
        Passed from input() function

    Returns: 
        bool:  True/False

    Raises:
    ValueError: "The answer was not recognised" if user input is not one of: yes, no, YES, NO, True, False
    
    """
    accepted_inputs = {"y": True,
                       "yes": True,
                       "n": False,
                       "no": False}
    input = accepted_inputs[input.lower()]
    if input == True or input == False:
        return (input)
    else:
        raise ValueError("Sorry, that answer was not recognised. Please try again")


def date_into_timestamp(date):
    """
    Turns specified date into timestamp corresponding to epochs in the database.

    Parameters:
        date: str
            In the format: YYYY-MM-DD-hh-mm
    
    Returns: 
        int: timestamps to be used as epochs
    
    Raises:
        ValueError: "The date you provided is not a valid timestamp. Please try again" if the specified numbers do not form a valid date. 
        ValueError: "You provided the date(s) in the wrong format. Please try again using the format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm" if the formatting for the date is wrong
    
    """
    try:
        datepars = list(map(int, date.split('-')))  # Convert string input into numeric list
        if len(datepars) <= 3:
            datepars.extend((0,0))
    except:
        raise ValueError("You provided the date(s) in the wrong format. Please try again using the format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm")
    try:
        return (int(datetime(*datepars).timestamp()))
    except:
        raise ValueError("The date you provided is not a valid timestamp. Please try again")
    

def return_dates(timeinp):
    """
    Parses dates into timestaps. Can handle two datetimes, two dates, or one date.
    If only one date (but no time) is specified, start_date will be the specified date at time 00:00 and end_date will be the specified date at time 23:59
    If two dates (but no time) are specified, start_date will be the specified start date at time 00:00 and end_date will be the specified end date also at 00:00
    
    Parameters:
        timeinp: str
            In the format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm OR YYYY-MM-DD OR YYYY-MM-DD:YYYY-MM-DD

    Returns: 
        start_date: int (via helper function date_into_timestamp), timestamp corresponding to epochs in the database (if data from this epoch exists/has been added to the database).
        end_date: int (via helper function date_into_timestamp), timestamp corresponding to epochs in the database (if data from this epoch exists/has been added to the database).
    """
    if ':' in timeinp:  # if the separator is included in the query
        dates = timeinp.split(':')  # split two dates into list
        start_date = date_into_timestamp(dates[0])
        end_date = date_into_timestamp(dates[1])  # and define different end-date
    else:  # if there is no separator in the query, turn the same date into two timestamps
        start_timeinp = timeinp + "-0" + "-0" # since it is only one date, we want to grab entries from start timestamp 00:00
        start_date = date_into_timestamp(start_timeinp)
        end_timeinp = timeinp + "-23" + "-59" # and, we need to make the end timestamp be at time 23:59 of the specified date
        end_date = date_into_timestamp(end_timeinp)
    return start_date, end_date


def validate_filename(filenames, toolname):
        """
        Checks if filenames are suitable for this tool. E.g.: p1g files only for p1g tool.
        Also checks if files are of a suitable format. E.g.: .csv, .tsv, .csv.gz and .tsv.gz files.
        
        Parameters:
            filenames: list
                Filenames to check
            toolname: str
                Which tool called this function

        Returns:
            str: valid filename for tool currently in use
        
        Raises:
            ValueError: "{filename} is not a valid {toolname} filepath!"
            if the specified filepath is the only one specified by the user and does not correspond to a datafile compatible with the tool which called it.
        """
        valid_filepaths = []
        if len(filenames) == 1:
            if toolname not in str(filenames[0]): # if we are trying to insert a file with the wrong name 
                raise ValueError(f"{filenames[0]} is not a valid {toolname} filepath!  Please enter a valid {toolname} filepath.")
            elif ".csv" not in str(filenames[0]) and ".tsv" not in str(filenames[0]): # if we are trying to insert something that is not a wildcard but is not the correct file format (thus not .csv, .tsv, .csv.gz, .tsv.gz)
                raise ValueError(f"{filenames[0]} is not a valid {toolname} filepath!  Please enter a valid {toolname} filepath.")
            else:
                valid_filepaths.append(filenames[0])
        else:
            for filename in filenames:
                if toolname not in str(filename):
                    click.echo(f"{filenames[0]} is not a valid {toolname} filepath!  Please enter a valid {toolname} filepath.") # here, we want to echo the error instead of raising it, as we do not want it to interrupt the script if only one of many files is not suitable
                elif ".csv" not in str(filename) and ".tsv" not in str(filename): #if the file is not a .csv, .tsv (or a compressed version of either), then it's not a valid file format
                    click.echo(f"{filename} is not an accepted file format! This file will be skipped")
                else:
                    valid_filepaths.append(filename)
        return(valid_filepaths)

    

def check_filepaths(user_input_files, toolname):
        """
        Fetches valid filepaths based on user's input. Can handle single filename, and wildcard names with asterisk.
        
        Parameters:
            user_input_files: str or tuple 
                String of filename the user wants inserted/or the wildcard string, or tuple passed from the CLI tools. 
            toolname: str
                Which tool called this function

        Raises: 
            Exception: "No files matching the specified pattern found! Please specify a valid {toolname} filepath." 
                If no files matching the specified filename are found in the directory.
            ValueError: "(One of) the file(s) {file} specified is not a valid file/is corrupted. Please enter a valid {toolname} filepath."
                If the filename entered is not suitable for this tool (e.g.: P1g file tried through the smartthings tool).
            Exception: "No files specified! Please specify (a) filename(s) to insert!"
                If the user has not provided any filenames to insert

        Returns:
            List of one or multiple filenames
        """
        if len(user_input_files) == 1 and (".py" in user_input_files[0] or "*" in user_input_files[0]): # if user provided only 1 file as an argument and if ".py" is its extension, or if the user specifies the wildcard in quotations (macOS) then no data files are found in this directory (meaning the data is stored in a 'data' folder)
            if toolname not in str(user_input_files[0]):
                raise ValueError(f"{user_input_files} is not a valid {toolname} filepath! Please enter a valid {toolname} filepath.")
            else:
                script_dir = os.path.dirname(os.path.realpath(__file__)) # therefore, the following lines enter a directory called "data", then the directory of the toolname
                tool_dir = os.path.join(script_dir, 'data', toolname) # so with these lines we are able to fetch data from a directory structure like the one in which the data was uploaded to Brightspace
                full_path = os.path.join(tool_dir, user_input_files[0])
                files = glob.glob(full_path) # now fetch all files from this directory
                valid_filepaths = validate_filename(files, toolname)
                if len(valid_filepaths) > 0:
                    return(valid_filepaths)
                else:
                    raise Exception(f"No files matching the specified pattern found! Please specify a valid {toolname} filepath.") # if no matching files were found in this data directory either
        elif len(user_input_files) == 0:
            raise Exception("No files specified! Please specify (a) filename(s) to insert!")
        else:
            valid_filepaths = validate_filename(user_input_files, toolname)
            if len(valid_filepaths) > 0:
                return(valid_filepaths)
            else:
                raise Exception(f"No files matching the specified pattern found! Please specify a valid {toolname} filepath.")
        


def timestamp_into_ams_time(timestamp):
    """
    Takes UNIX epoch timestamp and converts into datetime in Europe/Amsterdam time
    
    Parameters:
        timestamp: float
            Timestamp from datetime.timestamp() function
    
    Returns:
        datetime.datetime object
            Datetime in Europe/Amsterdam (Noordwijk time)
    
    """
    return(datetime.fromtimestamp(timestamp, ZoneInfo("Europe/Amsterdam")))


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

                    logging.error(f"Could not insert device {devices.loc[i,'name']}: {e}")
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
        P1g.dropna(inplace=True)

        # Create table if it was dropped
        self.create_p1g_table()
        
        # Temporary table for aggregation purposes
        with self.db.begin() as connection:
            P1g.to_sql("temp", connection, if_exists="replace", index=False)
            agg_query = sa.text("""SELECT epoch, 
                        avg(Total_gas_used) as Total_gas_used
                        FROM temp
                        WHERE epoch NOT IN (SELECT epoch FROM P1g)
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
        Method handling queries to the database. 
        Takes in SQL query as string, returns a pandas dataframe with the query and allows saving query result as csv.

        Parameters:
            self.db: HomeMessagesDB object
                The engine variable needed to start the connection
            query: str
                The desired query to be carried out
            save_file: bool
                Option to save the file as a CSV, default to False
        
        Returns:
            pd.DataFrame: Dataframe resulting from the query ran
        
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
                    logging.info("Table dropped successfully")
                    delete_query = sa.text(f"DELETE FROM tracking WHERE file_name LIKE '%{table_name}%'")
                    connection.execute(delete_query)
                else:
                    logging.error(f"Table {table_name} does not exist in the database {self.url}.")
    


    def erase_table_content(self, table_name, ask = True, message = True):
        """
        Function handling table deletions. 
        Deletes the data from the table and removes the corresponding file name from the 'tracking' table.

        Asks user to confirm whether they would like to erase all content from the table before proceeding.

        Parameters:
        - self.db: The engine variable needed to start the connection
        - table_name: The name of the table to delete the data from
        """
        if ask:
            click.echo("Are you sure you want to erase all contents of this table from the database? Y/N")
            inp = parse_user_answer(input())
        if (not ask) or inp:
            with self.db.begin() as connection:
                table_names = sa.text(f"SELECT name FROM sqlite_master WHERE type='table' and tbl_name = '{table_name}'")
                tables = connection.execute(table_names).fetchone()
                if tables:
                    try:
                        erase_query = sa.text(f"DELETE FROM {table_name} WHERE 1=1")
                        connection.execute(erase_query)
                        delete_query = sa.text(f"DELETE FROM tracking WHERE file_name LIKE '%{table_name}%'")
                        connection.execute(delete_query)
                        if message == True:
                            click.echo(f"Data in table {table_name} deleted successfully")
                        
                    except Exception as e:
                        logging.error(f"Could not erase the content of table {table_name}: {e}")
                        click.echo(f"Could not erase the content of table {table_name}: {e}")
                        raise e
                else:
                    logging.error(f"Table {table_name} does not exist in the database {self.url}.")
                    click.echo(f"Table {table_name} does not exist in the database {self.url}.")
        else:
            click.echo("Ok, table not erased.")

    

    def query_size(self, table_name):
        """
        Queries the size of the specified table from the database. This method is called in the Tool scripts.

        Parameters:
            table_name: str
                The table whose size we wish to find out.
        
        Raises:
            Exception: "Could not get the dimensions for this data. Error: " plus the error which stops us from fetching the table size.
        """
        try:
            temp = self.query_db(f"SELECT * FROM '{table_name}'")
            click.echo(f"The {table_name} table has {temp.shape[0]} rows and {temp.shape[1]} columns")
        except Exception as e:
            click.echo(f"Could not get the dimensions for this data, Error: {e}")
    

    def return_whole_table(self, table_name):
        """
        Returns the whole specified table from the database as a pandas dataframe

        Parameters:
            table_name: str
                Table to fetch from the database.
        
        Returns:
            pandas.dataframe: containing all data from the table corresponding to the parameter passed to the function.
        """
        return(pd.DataFrame(self.query_db(f"SELECT * FROM '{table_name}'")))


    def query_electricity(self):
        """
        Queries electricity consumption from the P1e table in the database. Allows user to specify either import, export, or both.

        """
        elec_inp = input("Do you want electricity: Import/Export/Export & Import")
        if(elec_inp.lower() == " import"):
            query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import FROM P1e"
            output = self.query_db(query)
            click.echo(f"the average {elec_inp} was {output}")
        elif(elec_inp.lower() == " export"):
            query = f"SELECT AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM P1e"
            output = self.query_db(query)
            click.echo(f"the average {elec_inp} was {output}")
        elif(elec_inp.lower() == " export & import"):
            query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import, AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM '{tablename}'"
            output = self.query_db(query)
            click.echo(f"the average {elec_inp} was {output}")
        else:
            click.echo("Invalid input, please try again specifying import/export/both")



    def query_device(self, name_inp = None, dataframe = False):
        """
        Queries entries with a specific device name from the database. Currently specific to the Smartthings table.

        If used by the scripts/tools (with default arguments dataframe = False and name_inp = None), outputs the result on the console.
        If used in the reports (with argument dataframe = True and name_inp passed), returns the result in a Pandas dataframe.

        Parameters:
            tablename: str
                Table from which to query
            name_inp: None
                This parameter is None by default, but this variable collects user's input from the Terminal to query entries of this device if it is called from the smart_things tool script
            dataframe: bool
                This parameter is False by default, but SHOULD BE SET TO TRUE in order to fetch data for certain devices as a dataframe for the reports.

        Raises:
            Exception: "Could not fetch the data for this device: {name_inp}" + the error message interrupting the process.
        
        Returns:
            pandas.Dataframe: Containing the entries in the smartthings table pertaining to the device specified
        """
        if name_inp == None:
            name_inp = input("Which device name do you want to filter the dataset for?")
        try:
            query = f"SELECT * FROM smartthings WHERE name = '{name_inp}'"
            output = self.query_db(query)
            click.echo(f"the device {name_inp} has the following values: {output}")
        except Exception as e:
            click.echo(f"Could not fetch the data for this device: {name_inp}, Error: {e}")
        if dataframe == True:
            return(pd.DataFrame(output))



    def query_average_gas(self):
        """
        Displays average gas usage between two dates according to data currently in the database. Only applicable to P1g table.

        If the database does not contain entries between these dates, then the average use output will be 0.

        """
        click.echo("From when to when? In format: YYYY-mm-dd. YYYY-mm-dd:YYYY-mm-dd. You may also specify a single date by ommitting everything after the colon")
        timeinp = input()
        start_date, end_date = return_dates(timeinp)
    
        average = self.query_db(f'''SELECT AVG(Total_gas_used) AS average_value
                            FROM P1g
                            WHERE epoch >= {start_date} AND epoch <= {end_date}''')
        click.echo(average)


    def return_entries_between_dates(self, toolname, time_inp = None, dataframe = False, save_file = False):
        """
        Queries data from specific date or between specific datetimes, based on user input.
        Allows user to save it to a file if desired.

        If used by the scripts/tools (with default argument dataframe = False and timeinp = False), outputs the result on the console (and if required saves to file).
        If used in the reports (with argument dataframe = True and timeinp specified), returns the result in a Pandas dataframe.

        Parameters:
            time_inp: str 
                Must be in format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm OR YYYY-MM-DD OR YYYY-MM-DD:YYYY-MM-DD
            dataframe: bool
                When called from the command-line tools, not specified so remains False. When used for reports, should be set to True to acquire pandas dataframes.
            save_file: bool
                When called from reports, should not be specified so that it remains False. When called from the command line tools, it will ask the user if they would like to save to a file.
        
        Returns:
            (optional) pandas.dataframe: with results from the query
        """
        if time_inp == None:
            click.echo('''From when until when? 
                    In format: YYYY-MM-DD-hh-mm:YYYY-mm-dd-hh-mm. 
                    You may also omit hh-mm. The entries will then start and end at 00:00.
                    You may also get results for a single date by ommitting everything after the colon''')
            time_inp = input()

            click.echo("Would you like to save the output to a file? Y/N")
            save_file = parse_user_answer(input())
        
        start_date, end_date = return_dates(time_inp)

        result = self.query_db(f'SELECT * FROM {toolname} WHERE epoch >= {start_date} AND epoch <= {end_date}', save_file)

        if result.shape[0] == 0:
            click.echo("No results found for those dates!")
        elif dataframe == True:
            return(pd.DataFrame(result))
        else:
            click.echo(result)

    def insert_all(self):
            """
            This method inserts all files that are given as argument in the correct table. Created
            for the reports. 

            Returns:
                None
            """

            # Erasing all content   
            self.erase_table_content("smartthings", ask = False, message = False)
            self.erase_table_content("P1e", ask = False, message = False)
            self.erase_table_content("P1g", ask = False, message = False)
                        
            # Adding all tables
            files = check_filepaths(("P1e*",),"P1e")
            for file in files:
                self.insert_table_P1e(file)
            files = check_filepaths(("smartthings*",),"smartthings")
            for file in files:
                self.insert_table_smartthings(file)
            files = check_filepaths(("P1g*",),"P1g")
            for file in files:
                self.insert_table_P1g(file)



    































    










# Essentials for Data Science group assignment

# Documentation

## Global functions for use in scripts and reports

#### *parse_user_answer(input)*
This function parses user answer to yes or no questions. Raises error if input cannot be understood.
    Parameters:
        input: str
            Passed from input() function
    Raises:
        ValueError: "The answer was not recognised" if user input is not one of: yes, no, YES, NO, True, False

    Returns: 
        bool:  True/False

#### *date_into_timestamp(date)*
This function turns a specified date (in format: YYYY-MM-DD-hh-mm) into a timestamp to be used as epoch in the database. Raises Exception if date was specified in the wrong format. 
    Parameters:
        date: str
            In the format: YYYY-MM-DD-hh-mm
    
    Raises:
        ValueError: "The date you provided is not a valid timestamp. Please try again" if the specified numbers do not form a valid date. 
        ValueError: "You provided the date(s) in the wrong format. Please try again using the format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm" if the formatting for the date is wrong
    
    Returns: 
        int: timestamps to be used as epochs

#### *return_dates(timeinp)*
This function parses dates into timestaps. Can handle two datetimes, two dates, or one date.
If only one date (but no time) is specified, start_date will be the specified date at time 00:00 and end_date will be the specified date at time 23:59
If two dates (but no time) are specified, start_date will be the specified start date at time 00:00 and end_date will be the specified end date also at time 00:00
    
    Parameters:
        timeinp: str
            In the format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm OR YYYY-MM-DD OR YYYY-MM-DD:YYYY-MM-DD

    Returns: 
        start_date: int (via helper function date_into_timestamp), timestamp corresponding to epochs in the database (if data from this epoch exists/has been added to the database).
        end_date: int (via helper function date_into_timestamp), timestamp corresponding to epochs in the database (if data from this epoch exists/has been added to the database).

#### *validate_filename(filename, toolname)*
This function checks if the filename specified by the user is suitable for this tool. E.g.: p1g files only for p1g tool. 
      Parameters:
          filename: str
              Filename specified by the user as input to the command-line tools 
          toolname: str
              Which tool called this function
        
      Raises:
          ValueError: "{filename} is not a valid {toolname} filepath!" 
          if the specified filepath does not correspond to a datafile compatible with the tool which called it.
      
      Returns:
          str: valid filename for tool currently in use

#### *check_filepaths(user_input_files, toolname)*
This function fetches valid filepaths based on user's input. Can handle single filename, and wildcard names with asterisk.
        Parameters:
            user_input_files: str
                String of filename the user wants to input into the database, or the wildcard query for this tool.
            toolname: str
                Which tool called this function

        Raises: 
            Exception: "No files matching the specified pattern found! Please specify a valid {toolname} filepath." 
                If no files matching the specified filename are found in the directory.
            ValueError: "(One of) the file(s) {file} specified is not a valid file/is corrupted. Please try again."
                If the filename specified corresponds to a corrupt file/not a file.

        Returns:
            List of one or multiple filenames

#### *timestamp_into_gmt2(timestamp)*
This function takes NIX epoch timestamp and converts into datetime in GMT+2 timezone

    Parameters:
        timestamp: float
            Timestamp from datetime.timestamp() function
    
    Returns:
        datetime.datetime object
            Datetime in GMT-2 (Noordwijk time)


## HomeMessagesDB class methods

The class HomeMessagesDB is initialised with a database URL. For the purpose of this documentation, the instance of the class will be called `database`. Each method includes exception handling, for a clear identification of the issue.

### Methods for creating and modifying the database

#### *database.create_db()*

This method allows the user to connect to a database, or to create a database with the provided URL if it does not exist, and then connecting to it through an engine. Following this, it creates five empty tables:

* `smartthings`
* `p1e`
* `p1g`
* `devices`
  + Table containing the information of each home device
* `tracking`
  + Table keeping track of which files have already been inserted into the database, in order to avoid duplicates

#### *database.create_smartthings_table()*

This method creates a table in the database with the structure:

* `id` (incremental primary key)
* `name`
* `epoch`
* `capability`
* `attribute`
* `unit`
* `value_int`
* `value_str`

The method then creates a table `devices` where the information about the smart devices is stored. The table is structured as follows:

* `name` (primary key and foreign key to `smatthings`)
* `level`
* `loc`

#### *database.create_p1e_table()*

This method creates a table in the database with the structure:

* epoch (primary key)
* Electricity_imported_T1
* Electricity_imported_T2
* Electricity_exported_T1
* Electricity_exported_T2

Additionally, the method checks whether the table `smartthings` exists; if it does, then `epoch` is declared as foreign key to `smartthings`.

#### *database.create_p1g_table()*

This method creates a table in the database with the structure:

* epoch (primary key)
* Total_gas_used

Additionally, the method checks whether the table `smartthings` exists; if it does, then `epoch` is declared as foreign key to `smartthings`.

#### *database.insert_table_smartthings(file)*

This methods takes as argument the name of a `smartthings` file and stores the information in two Pandas dataframes: `smartthings` and `devices`. The dataframe `smartthings` has then the following alterations:

* The variable `epoch` is transformed into an epoch number
* The variable `value` is separated into `value_int`, which holds all the numerical values, and `value_str`, which holds all the string values
* The variables `loc`, `level, and `value` are dropped

Then, a `smartthings` table is created in case it was dropped or it did not exist. Subsequently, the method queries the tracking file to see if the file had already been added to the database; if it has, the method returns a warning, otherwise it adds the name of the file to the `tracking` table, and then it inserts the file into the `smartthings` table on the database.

In the second part of the method, the `devices` dataframe has the following alterations:

* All columns are dropped except `loc`, `level` and `name`
* Duplicates are dropped

Then, rows that are missing are appended to the `devices` table in the database.

#### *database.insert_table_p1e(file)*

This method takes as argument the file name and reads a single (zipped or simply) csv file and inputs it into a Pandas dataframe. It executes the following actions: 

* Converts the `time` variable into the `epoch` variables, which contains the epoch value of `time` 
* Renames the columns in order to have consistent nomenclature
* Drops rows for which all columns but `epoch` present NAs

Then, the method creates the table `p1e`


... [MORE METHODS FOR CREATING AND POPULATING THE DATABASE] ...


Note on OpenWeatherMap data: We decided to not include the OpenWeatherMap data into the database but instead get it directly from the source everytime, this will be handled in the OpenWeatherMap tool.

### Methods for querying the database

#### *query_db(query, save_file = False)*
This method takes as input a string that gets converted to an SQL text to be able to query from the database, and it returns the result as a pandas dataframe.
Takes in SQL query as string, returns a pandas dataframe with the query and allows saving query result as csv.
It has an additional argument "save_file" which when set to true, allows the user to save the query as a csv file in the current directory, the name of this file will be query_result_{current time} (e.g. query_result_2025-05-05_16_18_57.315004). 

        Parameters:
            query: str
                The desired query to be carried out
            save_file: bool
                Option to save the file as a CSV, default to False
        
        Returns:
            pd.DataFrame: Dataframe resulting from the query ran

#### *query_size(table_name)*
This method Queries the size of the specified table from the database. It is called in the Tool scripts.
        Parameters:
            table_name: str
                The table whose size we wish to find out.
        
        Raises:
            Exception: "Could not get the dimensions for this data. Error: " plus the error which stops us from fetching the table size.

#### *return_whole_table(table_name)*
**USEFUL FOR REPORTS** This method returns the whole specified table from the database as a pandas dataframe
        Parameters:
            table_name: str
                Table to fetch from the database.
        
        Returns:
            pandas.dataframe: containing all data from the table corresponding to the parameter passed to the function.

#### *query_electricity(tablename)*
This method queries electricity consumption from the P1e table in the database. Allows user to specify either import, export, or both.
        Parameters:
            tablename: str
                Table to fetch electricity consumption from. (Only P1e is supported.)

#### *query_device(tablename, name_inp = None, dataframe = False)*
**USEFUL FOR REPORTS** This method queries entries with a specific device name from the database. Currently specific to the Smartthings table.

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

#### *query_average_gas()*
This method displays average gas usage between two dates according to data currently in the database. Only applicable to P1g table.
If the database does not contain entries between these dates, then the average use output will be 0.

#### *return_entries_between_dates(self, toolname, time_inp = None, dataframe = False, save_file = False)*
**USEFUL FOR REPORTS** This method queries data from specific date or between specific datetimes, based on user input. It allows the user to save it to a file if desired.

If used by the scripts/tools (with default argument dataframe = False and timeinp = False), outputs the result on the console (and if required saves to file).
If used in the reports (with argument dataframe = True and timeinp specified), returns the result in a Pandas dataframe.
        Parameters:
            time_inp: str 
                Must be in format: YYYY-MM-DD-hh-mm:YYYY-MM-DD-hh-mm OR YYYY-MM-DD OR YYYY-MM-DD:YYYY-MM-DD
            dataframe: bool
                When called from the command-line tools, not specified so remains False. **When used for reports, should be set to True to acquire pandas dataframes.**
            save_file: bool
                **When called from reports, should not be specified so that it remains False.** When called from the command line tools, it will ask the user if they would like to save to a file.
        
        Returns:
            (optional) pandas.dataframe: with results from the query

**Extra functionality**:

Drop Table: drop_table: This method takes as argument the name of the table to be dropped, it drops the table entirely from the dataframe; simultaneously, it deletes from the tracking table, the entries corresponding to the dropped table. This function does not allow to drop the tracking table.

Delete table data: erase_table: This method takes as argument the name of the table from where the data will be deleted, it deletes all entries from said table and from the tracking table as well.

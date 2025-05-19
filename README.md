# Essentials for Data Science group assignment

## Documentation

### HomeMessagesDB class methods

The class HomeMessagesDB is initialised with a database URL. For the purpose of this documentation, the instance of the class will be called `database`. Each method includes exception handling, for a clear identification of the issue.

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

Note on OpenWeatherMap data: We decided to not include the OpenWeatherMap data into the database but instead get it directly from the source everytime, this will be handled in the OpenWeatherMap tool.

**Querying from the Database**: 
query_db: This method takes as input a string that gets converted to an SQL text to be able to query from the database, and it returns the result as a pandas dataframe. 
* It has an additional argument "save_file" which when set to true, allows the user to save the query as a csv file in the current directory, the name of this file will be query_result_{current time} (e.g. query_result_2025-05-05_16_18_57.315004). 

**Extra functionality**:

Drop Table: drop_table: This method takes as argument the name of the table to be dropped, it drops the table entirely from the dataframe; simultaneously, it deletes from the tracking table, the entries corresponding to the dropped table. This function does not allow to drop the tracking table.

Delete table data: erase_table: This method takes as argument the name of the table from where the data will be deleted, it deletes all entries from said table and from the tracking table as well.

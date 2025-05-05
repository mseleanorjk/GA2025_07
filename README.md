# Essentials for Data Science group assignment

URL to SQLite CLI instructions:
https://www.sqlite.org/quickstart.html

Distribution of tasks:

1 Database: Eleonora & Canela : Sunday Evening
1 Class & Modules (Homemessages): Eleonora & Canela : Monday Evening
SmartThings tool: Bashier & Eva : 12th May
P1e tool: Elena & Miro : 12th May
P1g tool: Elena & Miro : 12th May
Openweather tool: Bashier & Eva : 12th May

Meeting for feedback and check & Define questions: 13th May: 12:00pm - 13:30pm

First Stage (Development) : 13th May
Second Stage (Analysis) : 17th May
Final (Review & Feedback) : 18th May (Eva not available on 18th, Eleonora not on 19th)

Notes: Write docstrings and document functionality on Readme.

DOCUMENTATION:

**HomeMessagesDB Class methods**:

**Creating a relational database**:

Create / Connect to Database:_createdb_: This method takses the database url as argument and uses it to (create the database if it does not exist and) connect to it through an engine. It also:
* Creates the (empty) tables: devices, smartthings, p1e, p1g and tracking [with the purpose of keeping a record of all files put into the tables and avoid duplicated information]. 
Exception Handling: if SQL fails to create tables, an error will communicate the issue, allowing the user to identify where the issue happened to be able to address it.

**Insert data into the database**: 

_insert_table_smart_things_: This method takes as input the filename (or directory) and reads a single (zipped or simply) csv file and inputs it into the table. It executes the following actions: 
* Converts the epoch column from the input file to UNIX time.
* Separates the value column from the input file into value_txt (strings) and value_int(floats).
* Takes the information regarding location and level and ensure it's present in the devices table. 
* Inputs filenames into the tracking table. 
Exception Handling: In case the process of appending the data to the table fails, a clear error will inform the user of this.

_insert_table_P1e (same for _insert_table_P1g): This method takes as input the file name and reads a single (zipped or simply) csv file and inputs it into the table. It executes the following actions: 
* Converts the 'time' column to UNIX time, leaving only a column called 'epoch' that contains this information. 
* Renames the columns to replace the " " with "_". 
* Inputs filenames into the tracking table. 
Exception Handling: In case the process of appending the data to the table fails, a clear error will inform the user of this.

Note on OpenWeatherMap data: We decided to not include the OpenWeatherMap data into the database but instead get it directly from the source everytime, this will be handled in the OpenWeatherMap tool.

**Querying from the Database**: 
query_db: This method takes as input a string that gets converted to an SQL text to be able to query from the database, and it returns the result as a pandas dataframe. 
* It has an additional argument "save_file" which when set to true, allows the user to save the query as a csv file in the current directory, the name of this file will be query_result_{current time} (e.g. query_result_2025-05-05_16_18_57.315004). 

**Extra functionality**:

Drop Table: drop_table: This method takes as argument the name of the table to be dropped, it drops the table entirely from the dataframe; simultaneously, it deletes from the tracking table, the entries corresponding to the dropped table. This function does not allow to drop the tracking table.

Delete table data: erase_table: This method takes as argument the name of the table from where the data will be deleted, it deletes all entries from said table and from the tracking table as well.

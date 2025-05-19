import os
import click
import glob
import logging
from datetime import datetime
import pandas as pd

def validate_filename(filename, toolname):
    """
    Checks if filename is suitable for this tool. E.g.: p1g files only for p1g tool. Raises Exception if not.

    Returns:
        filename: valid filename for tool
    """
    if toolname not in filename:
        logging.error(f"Validate_filepath failed for {filename} in {toolname}; invalid filepath")
        raise Exception(f"{filename} is not a valid {toolname} filepath! Please enter a valid {toolname} filepath.")
    else:
        return filename


def check_filepaths(user_input_files, toolname):
    """
    Fetches valid filepaths based on user's input. Can handle single filename, and wildcard names with asterisk.

    Returns:
        List of one or multiple filenames
    """
    valid_filepaths = []
    script_dir = os.path.dirname(os.path.realpath(__file__))
    parent_dir  = os.path.abspath(os.path.join(script_dir, '..'))
    tool_dir = os.path.join(parent_dir, 'data', toolname)
    filename = validate_filename(user_input_files, toolname)
    full_path = os.path.join(tool_dir, filename)
    files = glob.glob(full_path)
    if len(files) == 0:
        raise Exception(f"No files matching the specified pattern found! Please specify a valid {toolname} filepath.")
    for file in files:
        if os.path.isfile(file):
            valid_filepaths.append(file)
        else:
            raise Exception(f"(One of) the file(s) {file} specified is not a valid file/is corrupted. Please try again.")
    return(valid_filepaths)


def file_insertion(files, mydb, toolname):
    """
    Inserts a file from the folder into the specified table in the Database

    """
    if toolname == "P1e":
        for file in files:
            mydb.insert_table_P1e(file)
    elif toolname == "P1g":
        for file in files:
            mydb.insert_table_P1g(file)
    elif toolname == "smartthings":
        for file in files:
            mydb.insert_table_smartthings(file)
    else:
        click.echo("Please provide a valid toolname")



def query_size(mydb, tableName):
    """
    Queries the size of the specified table from the MySQL database
    """
    try:
        temp = mydb.query_db(f"SELECT * FROM '{tableName}'")
        click.echo(f"The {tableName} table has {temp.shape[0]} rows and {temp.shape[1]} columns")
    except Exception as e:
        click.echo(f"Could not get the dimensions for this data, Error: {e}")


def query_electricity(mydb,tablename):
    """
    Queries electricity consumption from the P1e table in teh database. Allows user to specify either import, export, or both
    """
    elec_inp = input("Do you want electricity: Import/Export/Export & Import")
    if(elec_inp.lower() == " import"):
        query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import FROM '{tablename}'"
        output = mydb.query_db(query)
        click.echo(f"the average {elec_inp} was {output}")
    elif(elec_inp.lower() == " export"):
        query = f"SELECT AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM '{tablename}'"
        output = mydb.query_db(query)
        click.echo(f"the average {elec_inp} was {output}")
    elif(elec_inp.lower() == " export & import"):
        query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import, AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM '{tablename}'"
        output = mydb.query_db(query)
        click.echo(f"the average {elec_inp} was {output}")
    else:
        click.echo("Invalid input, please try again specifying import/export/both")


def query_name(mydb, tablename):
    """
    Queries the name of the specified table from the MySQL database. Currently specific to the Smartthings table
    """
    name_inp = input("Which device name do you want to filter the dataset for?")
    try:
        query = f"SELECT * FROM '{tablename}' WHERE name = '{name_inp}'"
        output = mydb.query_db(query)
        click.echo(f"the device {name_inp} has the following values: {output}")
    except Exception as e:
        click.echo(f"Could not fetch the data for this Name, Error: {e}")


def date_into_timestamp(date):
    """
    Turns specified date into timestamp corresponding to epochs in the database. Raises Exception if date was specified in the wrong format. 

    Returns: timestamps for epochs
    """
    try:
        datepars = list(map(int, date.split('-')))  # Convert string input into numeric list
        print("datepars: ", datepars)
    except:
        raise Exception(
            "You provided the date(s) in the wrong format. Please try again using the format: YYYY-mm-dd:YYYY-mm-dd")
    try:
        return (int(datetime(*datepars).timestamp()))
    except:
        raise Exception("The date you provided is not a valid timestamp. Please try again")


def return_dates(timeinp):
    """
    Parses dates into timestaps.
    
    Returns: valid timestamp (via helper function date_into_timestamp), corresponding to epochs in the database.
    """
    if ':' in timeinp:  # if the separator is included in the query
        dates = timeinp.split(':')  # split the list
        start_date = date_into_timestamp(dates[0])
        end_date = date_into_timestamp(dates[1])  # and define different end-date
    else:  # if there is no separator in the query
        start_date = date_into_timestamp(timeinp)
        end_date = start_date
    return start_date, end_date


def parse_user_answer(input):
    """
    Function to parse user answer to yes or no questions. Raises error if input cannot be understood.

    Returns: True or False
    """
    accepted_inputs = {"y": True,
                       "yes": True,
                       "n": False,
                       "no": False}
    input = accepted_inputs[input.lower()]
    if input == True or input == False:
        return (input)
    else:
        raise Exception("Sorry, that answer was not recognised. Please try again")


def erase(mydb,tableName):
    """
    Erases the specified table from the MySQL database
    """
    click.echo("Are you sure you want to erase all the contents of this table from the database? Y/N")
    inp = parse_user_answer(input())
    if inp == True:
        try:
            mydb.erase_table_content(tableName)
            click.echo(f"Successfully erased {tableName} content")
        except Exception as e:
            click.echo(f"Could not erase this table from the database: {e}")
    else:
        click.echo("Ok, table not erased")


def return_entries_between_dates(db, toolname):
    """
    Queries data from specific date or between specific dates, based on user input.
    Allows user to save it to a file if desired.

    Returns: 
        Data for specified dates.
    """
    
    click.echo("From when until when? In format: YYYY-mm-dd:YYYY-mm-dd. You may also specify a single date by ommitting everything after the colon")
    timeinp = input()
    start_date, end_date = return_dates(timeinp)

    click.echo("Would you like to save the output to a file? Y/N")
    save_file_option = parse_user_answer(input())

    result = db.query_db(f'SELECT * FROM {toolname} WHERE epoch >= {start_date} AND epoch <= {end_date}', save_file_option)
    if result.shape[0] == 0:
        click.echo("No results found for those dates!")
    else:
        click.echo(result)



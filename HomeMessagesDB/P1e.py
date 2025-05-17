import os
import click
import glob
import home_messages_db as db
import logging


def erase(mydb,tableName):
    """
    Erases the specified table from the MySQL database
    """
    try:
        mydb.erase_table_content(tableName)
        click.echo(f"Successfully erased {tableName} content")
    except Exception as e:
            click.echo(f"Error: {e}")


def query_size(mydb, tableName):
    """
    Queries the size of the specified table from the MySQL database
    """
    try:
        temp = mydb.query_db(f"SELECT * FROM '{tableName}'")
        click.echo(f"The P1e table has {temp.shape[0]} rows and {temp.shape[1]} columns")
    except Exception as e:
        click.echo(f"Could not get the dimensions for this data, Error: {e}")



def query_name(mydb, tableName):
    """
    Queries the name of the specified table from the MySQL database. Currently specific to the Smartthings table
    """


    name_inp = input("Which device name do you want to filter the dataset for?")
    try:
        query = f"SELECT * FROM tablename WHERE name = '{name_inp}'"
        output = mydb.query_db(query)
        click.echo(f"the device {name_inp} has the following values: {output}")
    except Exception as e:
        click.echo(f"Could not fetch the data for this Name, Error: {e}")

def query_electricity(mydb,tablename):
    elec_inp = input("Do you want electricity: Import/Export/Export & Import")
    if(elec_inp.lower() == " import"):
        query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import FROM '{tablename}'"
        output = mydb.query_db(query)
    elif(elec_inp.lower() == " export"):
        query = f"SELECT AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM '{tablename}'"
        output = mydb.query_db(query)
    elif(elec_inp.lower() == " export & import"):
        query = f"SELECT AVG((Electricity_imported_T1 +Electricity_imported_T2)/2) as avg_import, AVG((Electricity_exported_T1 +Electricity_exported_T2)/2) as avg_export FROM '{tablename}'"
        output = mydb.query_db(query)
    else:
        click.echo("Invalid input, please try again specifying import/export/both")
    click.echo(f"the average {elec_inp} was {output}")

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
    Fetches valid filepaths based on user's input. Can handle single filename, wildcard names with asterisk, or wildcard with question marks.

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
    if toolname == "P1e":
        for file in files:
            mydb.insert_table_P1e(file)
    elif toolname == "P1g":
        for file in files:
            mydb.insert_table_P1e(file)
    elif toolname == "smartthings":
        for file in files:
            mydb.insert_table_P1e(file)
    else:
        click.echo("Please provide a valid toolname")








CONTEXT_SETTINGS = dict(help_option_names=[' ','-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-d', 'dburl', metavar='DBURL', help= "Insert or remove into/from the project database (DBURL is a SQLAlchemy database URL)",required=True)
@click.option('-e','eraseTable',is_flag = True, help = "removes all data from the P1e table")
@click.option('-q', "query", is_flag = True, help = "Pass query to the P1e table")
@click.argument("filename", required = False, metavar = "P1e-2022-01-01-2022-05-07.csv.gz[...]")
def insert_file(dburl,filename, eraseTable, query):
    """
    This function can insert a datafile into the P1e table. It also allows for basic querying of the P1e table as well as allowing it to be erased.
    The order of the execution of the arguments is: Erase Table -> Insert File -> Query the P1e table

    Output options: "Insert or remove into/from the project database (DBURL is a SQLAlchemy database URL)"
    """


    try:
        mydb = db.HomeMessagesDB(dburl)
    except Exception as e:
        click.echo(f"Error: {e}")
    try:
        mydb.create_db()
    except Exception as e:
        click.echo(f"Error: {e}")

    if eraseTable:
        erase(mydb, "P1e")

    if filename:
        files = check_filepaths(filename, "P1e")
        try:
            file_insertion(files, mydb, "P1e")
        except Exception as e:
            click.echo(f"Error: {e}")

    if query:
        click.echo("What query would you like to run?")
        inp = input()
        if inp.lower() == "size":
            query_size(mydb, "P1e")
        elif inp.lower() == "electricity":
            query_electricity(mydb, "P1e")
        else:
            click.echo("Invalid input, please try again specifying what query you would like to run")




if __name__ == "__main__":
    insert_file()
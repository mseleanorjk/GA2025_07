import os
import click
import glob
import home_messages_db as db
import logging
from datetime import datetime

"""
-   When used without any arguments, or with -h or --help option, the tool should print a help message 
    describing the tool and its options (see the Click library).

-   When used with wrong arguments, the tool should raise an exception which would print an error message.

-   For top grades additional options can be added to the tools 
    (for example: show number of entries already present in the database, automatically handle compressed files, etc.). These additional options must be clearly documented in the README.md.


"""

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
    filename = validate_filename(user_input_files, "P1g")
    full_path = os.path.join(tool_dir, filename)
    files = glob.glob(full_path)
    if len(files) == 0:
        raise Exception(f"No files matching the specified pattern found! Please specify a valid {toolname} filepath.")
    for file in files:
        if os.path.isfile(file):
            valid_filepaths.append(file)
        else:
            raise Exception("(One of) the file(s) specified is not a valid file/is corrupted. Please try again.")
    return valid_filepaths


def date_into_timestamp(date):
    try:
        datepars = list(map(int, date.split('-'))) # Convert string input into numeric list
        print("datepars: ", datepars)
    except: 
        raise Exception("You provided the date(s) in the wrong format. Please try again using the format: YYYY-mm-dd:YYYY-mm-dd")
    try:
        return(int(datetime(*datepars).timestamp()))
    except:
        raise Exception("The date you provided is not a valid timestamp. Please try again")


def return_dates(timeinp):
    if ':' in timeinp: # if the separator is included in the query
        dates = timeinp.split(':') # split the list
        start_date = date_into_timestamp(dates[0]) 
        end_date = date_into_timestamp(dates[1]) # and define different end-date
    else: # if there is no separator in the query
        start_date = date_into_timestamp(timeinp)
        end_date = start_date
    return start_date, end_date



def parse_user_answer(input):
    accepted_inputs = {"y": True, 
                    "yes": True, 
                    "n": False, 
                    "no": False}
    input = accepted_inputs[input]
    return(input)
    


def return_dates(db):
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

    db.query_db(f'SELECT * FROM smartthings WHERE epoch => {start_date} AND epoch <= {end_date}', save_file_option)




    
def query_average_gas(db, tablename):
    click.echo("From when to when? In format: YYYY-mm-dd. YYYY-mm-dd:YYYY-mm-dd. You may also specify a single date by ommitting everything after the colon")
    timeinp = return_dates(timeinp)




@click.command()
@click.option('-d', '--dburl', required = True, help = 'DBURL into which to insert the database (must be a SQLAlchemy database URL)')
@click.option('-q', '--query', default=None, is_flag=True, help='Run a query instead of inserting files')

@click.argument("filename", required = False)
def p1g(dburl, filename, query):
    """
    Usage: 
        This script inserts gas consumption data from the P1g files into a SQLAlchemy database.

        p1g.py [OPTIONS] P1g-2022-12-01-2023-01-10.csv.gz [...]
    Output options: 
        -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
    """
    if query:
        return_dates()

    #filepath = check_filepaths(filename, "P1g")
    #click.echo(filepath)
    

if __name__ == "__main__":
    p1g()






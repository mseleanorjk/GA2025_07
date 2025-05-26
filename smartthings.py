from datetime import datetime
import click
import glob
import os
import home_messages_db as db


@click.command(no_args_is_help=True)
@click.option('-d','--dburl', help='DBURL into which to insert the database (must be a SQLAlchemy database URL)', required = True) # option -d for user
@click.option('-q','--query', default=None, is_flag=True, help='Run a query instead of inserting files')
@click.option('-qd', '--query_device', default=None, is_flag=True, help='Run a query fetching all entries for a certain device name')
@click.option('-e','--erasetable', default=None, is_flag=True, help='Removes all data from table')
@click.option('-s', '--size', default = None, is_flag = True, help = 'Output the current size (number of entries) of smartthings table in the database')
@click.argument('filename', nargs=-1, default = None, metavar = "smartthings.20250412.tsv.gz [...]")

def smartthings(dburl ,query ,query_device ,size ,erasetable ,filename):
    """
    Usage: 
        This script smart device data from the smartthings files into a SQLAlchemy database.
    Output options: 
        -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
        -e Remove all data from the smartthings table
        -q Run a query fetching entries between two datetimes or within a date or between two dates
        -qd Run a query fetching all entries for a certain device name
        -s Output the current size (number of entries) of the smartthings table in the database
    """
    # Initialize db
    mydb = db.HomeMessagesDB(dburl)
    mydb.create_db()
        
    # File gathering and insertion 
    if erasetable:
        mydb.erase_table_content("smartthings")
    
    elif query:
        mydb.return_entries_between_dates("smartthings")
    
    elif query_device:
        mydb.query_device()    
    
    elif size:
        mydb.query_size("smartthings")
    
    elif filename:
        files = db.check_filepaths(filename, "smartthings")
        with click.progressbar(files) as bar:
            for file in bar:
                try:
                    mydb.insert_table_smartthings(file)
                except Exception as e:
                    click.echo(f"Error- failed to insert file: {e}",err=True, nl=True)
    
if __name__ == '__main__':
    smartthings()

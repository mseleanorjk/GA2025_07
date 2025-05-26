import os
import click
import glob
import home_messages_db as db
import logging
from datetime import datetime



@click.command(no_args_is_help=True)
@click.option('-d', '--dburl', required = False, help = 'DBURL into which to insert the database (must be a SQLAlchemy database URL)')
@click.option('-e','--erasetable',is_flag = True, help = "removes all data from the P1e table")
@click.option('-q', '--query', default=None, is_flag=True, help='Run a query which fetches all entries in the P1e table occuring between two dates (or on one certain date)')
@click.option('-qa', '--query_average', default =None, is_flag = True, help = 'Fetch average electricity use between two dates (or on one specific date)')
@click.option('-s', '--size', default = None, is_flag = True, help = 'Output the current size (number of entries) of the P1e table in the database')
@click.argument("filename", required = False, default = None, nargs=-1, metavar = "P1e-2022-12-01-2023-01-10.csv.gz [...]")

def P1e(dburl, erasetable, query, query_average, size, filename):
    """
    Usage: 
        This script inserts electricity consumption data from the P1e files into a SQLAlchemy database.
    Output options: 
        -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
        -e Remove all data from the P1e table
        -q Run a query fetching entries between two datetimes or within a date or between two dates
        -qa Run a query fetching the average electricity usage between two dates or on one specific date
        -s Output the current size (number of entries) of the P1e table in the database
    """ 
    mydb = db.HomeMessagesDB(dburl)
    mydb.create_db()
    
    if erasetable:
        mydb.erase_table_content("P1e")
    
    elif query:
        mydb.return_entries_between_dates("P1e")
    
    elif query_average:
        mydb.query_electricity()
    
    elif size:
        mydb.query_size("P1e")
    
    elif filename:
        files = db.check_filepaths(filename, "P1e")
        with click.progressbar(files) as bar:
            for file in bar:
                try:
                    mydb.insert_table_P1e(file)
                except Exception as e:
                    click.echo(f"Error- failed to insert file: {e}",err=True, nl=True)
            
        
    

    

if __name__ == "__main__":
    P1e()
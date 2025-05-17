import os
import click
import glob
import home_messages_db as db
import logging
from datetime import datetime
import helperFile as hf




    
def query_average_gas(db):
    click.echo("From when to when? In format: YYYY-mm-dd. YYYY-mm-dd:YYYY-mm-dd. You may also specify a single date by ommitting everything after the colon")
    timeinp = input()
    start_date, end_date = hf.return_dates(timeinp)
    
    average = db.query_db(f'''SELECT AVG(Total_gas_used) AS average_value
                        FROM P1g
                        WHERE epoch >= {start_date} AND epoch <= {end_date}''')
    click.echo(average)



@click.command(no_args_is_help=True)
@click.option('-d', '--dburl', required = False, help = 'DBURL into which to insert the database (must be a SQLAlchemy database URL)')
@click.option('-e','--erasetable',is_flag = True, help = "removes all data from the P1g table")
@click.option('-q', '--query', default=None, is_flag=True, help='Run a query which fetches all entries in the P1g table occuring between two dates (or on one certain date)')
@click.option('-qa', '--query_average', default =None, is_flag = True, help = 'Fetch average gas use between two dates (or on one specific date)')
@click.option('-s', '--size', default = None, is_flag = True, help = 'Output the current size (number of entries) of the P1g table in the database')
@click.argument("filename", required = False, default = None, metavar = "P1g-2022-12-01-2023-01-10.csv.gz [...]")

def p1g(dburl, erasetable, query, query_average, size, filename):
    """
    Usage: 
        This script inserts gas consumption data from the P1g files into a SQLAlchemy database.
    Output options: 
        -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
        -e Remove all data from the P1g table
        -q Run a query (fetch all entries between 2 dates, fetch average electricity usage between 2 dates) instead  of inserting files
    """ 
    mydb = db.HomeMessagesDB(dburl)
    mydb.create_db()
    
    if erasetable:
        hf.erase(mydb, "P1g")
    
    elif query:
        hf.return_entries_between_dates(mydb, "P1g")
    
    elif query_average:
        query_average_gas(mydb)
    
    elif size:
        hf.query_size(mydb, "P1g")
    
    elif filename:
        files = hf.check_filepaths(filename, "P1g")
        hf.file_insertion(files, mydb, "P1g")


    

    

if __name__ == "__main__":
    p1g()



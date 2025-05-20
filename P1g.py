import os
import click
import glob
import home_messages_db as db
import logging
from datetime import datetime



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
        -q Run a query fetching entries between two datetimes or within a date or between two dates
        -qa Run a query fetching the average gas usage between two dates or on one specific date
        -s Output the current size (number of entries) of the P1g table in the database
    """ 
    mydb = db.HomeMessagesDB(dburl)
    mydb.create_db()
    
    if erasetable:
        mydb.erase_table_content("P1g")
    
    elif query:
        mydb.return_entries_between_dates("P1g")
    
    elif query_average:
        mydb.query_average_gas()
    
    elif size:
        mydb.query_size("P1g")
    
    elif filename:
        files = db.check_filepaths(filename, "P1g")
        for file in files:
            mydb.insert_table_P1g(file)
        
    

    

if __name__ == "__main__":
    p1g()



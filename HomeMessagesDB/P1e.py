import os
import sys
import click
import argparse
import glob
import home_messages_db as db


@click.command()
@click.option('-d' , '--dburl', required = True, help = 'DBURL insert into the project database (DBURL is a SQLAlchemy database URL)')
@click.argument("filename")
def insert_file(dburl,filename):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    main_dir = os.path.abspath(os.path.join(script_dir, '..'))
    p1e_dir = os.path.join(main_dir, 'data', 'P1e')
    filename = filename
    full_path = os.path.join(p1e_dir, filename)
    database = db.HomeMessagesDB(dburl)
    try:
        database.create_db()
    except Exception as e:
        print(f"Error: {e}")
    try:
        database.insert_table_P1e(full_path)
    except Exception as e:
        print(f"Error: {e}")
    print("Saving to:", os.path.abspath(dburl))
    return(database)



if __name__ == "__main__":
    insert_file()
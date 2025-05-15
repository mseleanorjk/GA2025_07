import os
import click
import glob
import home_messages_db as db

@click.command()
@click.option('-d' , '--dburl', required = True, help = 'DBURL insert into the project database (DBURL is a SQLAlchemy database URL)')
@click.argument("filename")
def insert_file(dburl,filename):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    main_dir = os.path.abspath(os.path.join(script_dir, '..'))
    p1e_dir = os.path.join(main_dir, 'data', 'P1e')
    full_path = os.path.join(p1e_dir, filename)
    files = glob.glob(full_path)
    try:
        database = db.HomeMessagesDB(dburl)
    except Exception as e:
        print(f"Error: {e}")
    try:
        database.create_db()
    except Exception as e:
        print(f"Error: {e}")
    for file in files:
        try:
            database.insert_table_P1e(file)
        except Exception as e:
            print(f"Error: {e}")
        



if __name__ == "__main__":
    insert_file()
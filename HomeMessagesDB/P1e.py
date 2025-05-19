import os
import click
import glob
import home_messages_db as db
import logging
import helperFile as hf
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
        hf.erase(mydb, "P1e")

    if filename:
        files = hf.check_filepaths(filename, "P1e")
        try:
            hf.file_insertion(files, mydb, "P1e")
        except Exception as e:
            click.echo(f"Error: {e}")

    if query:
        click.echo("What query would you like to run?")
        inp = input()
        if inp.lower() == "size":
            hf.query_size(mydb, "P1e")
        elif inp.lower() == "electricity":
            hf.query_electricity(mydb, "P1e")
        else:
            click.echo("Invalid input, please try again specifying what query you would like to run")




if __name__ == "__main__":
    insert_file()
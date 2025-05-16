#problems
#say your typing in your command line python smartthings.py -d dburl file1.tsv
#or in general smartthings.py -d dburl *tsv
#the code will not pick anything up as the files presented in the assig are tsv.gz
#should we build in a save net for that?
from datetime import datetime
import click
import glob
import os

@click.command() #function below should be treated as cli command
@click.option('-d','--dburl','d', help='flavor text as of now', required = True) # option -d for user
@click.option('--query', default=None, is_flag=True, help='Run a query instead of inserting files')
@click.argument('files',nargs=-1)

def main(d,files,query):
    """Very clear explanation of the function which will be written in the future"""
    
    from home_messages_db import HomeMessagesDB
    db = HomeMessagesDB(d)
    db.create_db()
    
    if query:
        click.echo('Would you like output specified on date, name or size of the database?')
        userinp= input()
        if userinp.lower() =='date':
            click.echo('from when until when? YYYY-mm-dd:YYYY-mm-dd')
            timeinp=input()
            if ':' in timeinp:
                dates=timeinp.split(':')
                datepars1=list(map(int, dates[0].split('-')))
                date1= int(datetime(*datepars1).timestamp())
                
                datepars2=list(map(int, dates[1].split('-')))
                date2=int(datetime(*datepars2).timestamp())
            else:
                datepars1=list(map(int, timeinp.split('-')))
                date1= int(datetime(*datepars1).timestamp())
                date2=date1

            output=db.query_db(f'SELECT * FROM smartthings WHERE epoch >= {1665346388} AND epoch <={1665346402}')
            click.echo(output)
        elif userinp=='all':
            output=db.query_db(f'SELECT * FROM smartthings')
            click.echo(output)
        elif userinp =="name":
            name_inp = input("Which name do you want to filter the dataset for?")
            query = f"SELECT * FROM smartthings WHERE name = '{name_input}'"
            output = db.query_db(query)
    click.echo(output)
        return
    
    all_files=[] #empty list to gather files from users input
    #loop over everything in files, user specified
    for pattern in files:
        #check if there is a pattern in what the user specified
        if '*' in pattern or '?' in pattern:
            #if so use it to find all files
            matched_files=glob.glob(pattern)
            #and add them to all_files list 
            all_files.extend(matched_files)
            
            #if glob does not find anything it gives back an empty list
            #therefore check if list matched files is empty and if so echo a warning 
            ## could be made better with Try and raiseError maybe
            ##although its cli so ???
            if len(matched_files)==0:
                click.echo('No files found using pattern!')
                return None
        
        #in the case the user wrote down filepaths without wildcards        
        else:    
            #check if path exists
            if os.path.exists(pattern):
                #add path to all_files
                all_files.append(pattern)
            else:
                click.echo(f'No file "{pattern}" found!')
                return None

    with click.progressbar(all_files, label='inserting files') as bar:
        for files in bar:
            db.insert_table_smartthings(files)

## this does not work as -d gets the dburl, so add a second option to dburL?
## No def make a new command for this  
    

if __name__ == '__main__':
    main()
    


int(datetime(*[2021,7,26]).timestamp())


timeinp=input()
print(timeinp)
datepars1=list(map(int, timeinp.split('-')))
print(datepars1)
date1= int(datetime(*datepars1).timestamp())
print(date1)

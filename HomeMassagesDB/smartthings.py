#problems
#say your typing in your command line python smartthings.py -d dburl file1.tsv
#or in general smartthings.py -d dburl *tsv
#the code will not pick anything up as the files presented in the assig are tsv.gz
#should we build in a save net for that?
#from home_messages_db.py import home_messages_db.py
import click
import glob
import os

@click.command() #function below should be treated as cli command
@click.option('-d','--dburl','d', help='flavor text as of now') # option -d for user
@click.argument('files',nargs=-1)

def present_files(d,files):
    """Very clear explanation of the function which will be written in the future"""
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
        
        #in the case the user wrote down filepaths without wildcards        
        else:    
            #check if path exists
            if os.path.exists(pattern):
                #add path to all_files
                all_files.append(pattern)
            else:
                click.echo(f'No file "{pattern}" found!')
                
    #add to database
    #db = home_messages_db(d)
    for f in all_files:
        #insert_table_smartthings(f)
        ## this is just to check if it works, echo will be deleted later 
        click.echo(f'{d},{f}')
        

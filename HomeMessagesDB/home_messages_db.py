import sqlalchemy as sa
import pandas as pd


class HomeMessagesDB:
    def __init__(self, url):
        self.url = url

    def create():
        """Create Database if it doesn't exist"""
        if not FileExists(self.url):
            db = create_engine(self.url)
    def insert_table_smartthings():
        """Create (if it doesnt exist) tables of smart things and devices"""
        %%sql
        CREATE TABLE IF NOT EXISTS devices (
            name TEXT PRIMARY KEY,
            loc TEXT NOT NULL,
            level TEXT NOT NULL,
            PRIMARY KEY (name)
            FOREIGN KEY (name) 
                REFERENCES smartthings (name)
        )
        %%sql
        CREATE TABLE IF NOT EXISTS smartthings (
            name TEXT NOT NULL,
            epoch TEXT NOT NULL,
            capability TEXT NOT NULL,
            attribute TEXT NOT NULL,
            value TEXT NOT NULL,
            unit TEXT,
            PRIMARY KEY (name,epoch,attribute)
            FOREIGN KEY (name) 
                REFERENCES devices (name)
        )
    def insert_table_p1e():
        """Create p1e table if it does not exist"""
        %%sql
        CREATE TABLE IF NOT EXISTS p1e (
            epoch INTEGER PRIMARY KEY,
            ImportT1kWh FLOAT,
            ImportT2kWh FLOAT,
            ExportT1kWh FLOAT,
            ExportT2kWh FLOAT,
            PRIMARY KEY (epoch)
            FOREIGN KEY (epoch) 
                REFERENCES smartthings (epoch)
        )
    def insert_table_p1g():
        """Create p1g table if it does not exist"""
        %%sql
        CREATE TABLE IF NOT EXISTS p1g (
            epoch INTEGER PRIMARY KEY,
            Total_gas_used FLOAT,
            PRIMARY KEY (epoch)
            FOREIGN KEY (epoch) 
                REFERENCES smartthings (epoch)
        )
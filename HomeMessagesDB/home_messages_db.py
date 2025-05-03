import sqlalchemy as sa
import pandas as pd


class HomeMessagesDB:
    def __init__(self, url):
        self.url = url

    def create():
        if not FileExists(self.url):
            db = create_engine(self.url)
    
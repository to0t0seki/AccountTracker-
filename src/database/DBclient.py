import sqlite3
import pandas as pd

class DBclient:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
    
    def __del__(self):
        self.conn.close()
    
    def execute(self, query, params=()):
        self.cursor.execute(query, params)
        self.conn.commit()
    
    def fetchall(self):
        return self.cursor.fetchall()
    
    def fetchone(self):
        return self.cursor.fetchone()
    
    def close(self):
        self.conn.close()

    def query_to_df(self, query, params=()):
        return pd.read_sql_query(query, self.conn, params=params)

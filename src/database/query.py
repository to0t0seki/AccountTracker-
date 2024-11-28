import pandas as pd
import src.database.DBclient as DBclient


def test():
    db_client = DBclient.DBclient("data/geek_transfers.db")
    query = "SELECT * FROM transactions"

    df = db_client.query_to_df(query)
    print(df)



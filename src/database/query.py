import pandas as pd
import src.database.DBclient as DBclient


def fetch_senders_to_bitget():
    db_client = DBclient.DBclient("data/geek_transfers.db")
    query = """
    SELECT distinct from_address
    FROM transfer_details
    WHERE to_address = '0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23'
    """
    df = db_client.query_to_df(query)
    return df

def fetch_recepients_from_airdrop():
    db_client = DBclient.DBclient("data/geek_transfers.db") 
    query = """
    SELECT distinct to_address
    FROM airdrops
    """
    df = db_client.query_to_df(query)
    return df
def fetch_addresses_received_multiple_airdrops():
    """
    Bitgetへ送金したアドレスの中で、2つ以上のAirdropアドレスからトークンを受け取ったアドレスを取得する
    
    Returns:
        DataFrame: 複数のAirdropから受け取ったアドレスのリスト
    """
    db_client = DBclient.DBclient("data/geek_transfers.db")
    query = """
    WITH bitget_senders AS (
        SELECT DISTINCT from_address
        FROM transfer_details
        WHERE to_address in('0x1AB4973a48dc892Cd9971ECE8e01DcC7688f8F23',"0x0D0707963952f2fBA59dD06f2b425ace40b492Fe")
    )
    SELECT td.to_address, COUNT(DISTINCT td.from_address) as airdrop_count
    FROM transfer_details td
    INNER JOIN bitget_senders bs ON td.to_address = bs.from_address
    INNER JOIN airdrops a ON td.from_address = a.to_address
    GROUP BY td.to_address
    HAVING COUNT(DISTINCT td.from_address) >= 2
    ORDER BY airdrop_count DESC
    """
    return db_client.query_to_df(query)

def fetch_game_addresses_of_multiple_airdrops(addresses):
    query = """
    with airdrop as (SELECT distinct to_address FROM airdrops)
    SELECT  td.from_address
    FROM transfer_details td
    join airdrop a on td.from_address = a.to_address
    where td.to_address = ?
    group by td.from_address
    """
    db_client = DBclient.DBclient("data/geek_transfers.db")
    df = db_client.query_to_df(query, (addresses,))
    return df

def fetch_details_of_airdrops(addresses:list):
    query = """
    SELECT sum(value) as total_value
    FROM airdrops
    WHERE to_address in ({})
    """.format(",".join(map(lambda x: "'{}'".format(x), addresses)))
    db_client = DBclient.DBclient("data/geek_transfers.db")
    df = db_client.query_to_df(query)
    return df
def fetch_details_of_withdrawals(addresses:list):
    query = """
    SELECT sum(value) as total_value
    FROM export_token
    WHERE to_address in ({})
    """.format(",".join(map(lambda x: "'{}'".format(x), addresses)))
    db_client = DBclient.DBclient("data/geek_transfers.db")
    df = db_client.query_to_df(query)
    return df

def fetch_details_of_deposits(addresses:list):
    query = """
    SELECT sum(value) as total_value
    FROM xgeek_to_geek
    WHERE from_address in ({})
    """.format(",".join(map(lambda x: "'{}'".format(x), addresses)))
    db_client = DBclient.DBclient("data/geek_transfers.db")
    df = db_client.query_to_df(query)
    return df
# df = fetch_senders_to_bitget()
# df = fetch_recepients_from_airdrop()
# df = fetch_addresses_received_multiple_airdrops()
df = fetch_game_addresses_of_multiple_airdrops("0x55058A5CB88B82b4e2C9B1c2BD97a3923Ab92e9c")
df_a = fetch_details_of_airdrops(df["from_address"].tolist())
df_w = fetch_details_of_withdrawals(df["from_address"].tolist())
df_d = fetch_details_of_deposits(df["from_address"].tolist())
print(df_a)
print(df_w)
print(df_d)


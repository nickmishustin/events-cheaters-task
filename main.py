from sqlite3.dbapi2 import Timestamp
import pandas as pd
import sqlite3
from datetime import date, datetime
import tracemalloc
import linecache
import os

def get_target_df(target_date, path):
    df= pd.read_csv(path, delimiter=',', header=0)
    df['date'] = df['timestamp'].apply(
        lambda T: datetime.fromtimestamp(T).date()
    )
    df = df[df['date'] == target_date]
    df.drop('date', axis=1, inplace=True)
    return df


def removed_cheaters(path, df):
    sql_connection = sqlite3.connect(path)
    df_cheaters =  pd.read_sql_query('SELECT * FROM cheaters', sql_connection)
    df = df_cheaters.merge(df, left_on='player_id', right_on='player_id')
    sql_connection.close()
    df['ban_time'] = df['ban_time'].apply(
        lambda T: datetime.strptime(T, '%Y-%m-%d %H:%M:%S')
    )
    df['server_time'] = df['timestamp'].apply(
        lambda T: datetime.fromtimestamp(T)
    )
    df['datetime_diff']  = (df['ban_time'] - df['server_time']).dt.days 
    df = df[df['datetime_diff'] >= 1]
    
    df.drop(
        ['ban_time', 'server_time', 'datetime_diff'],
        axis=1,
        inplace=True
    )
    
    return df

def main():
    target_date = date(year=2021, month=3, day=7)
    df_server = get_target_df(target_date, 'server.csv')
    df_client = get_target_df(target_date, 'client.csv')
    df = df_server.merge(df_client, left_on='error_id', right_on='error_id')
    df.rename(
        columns={
            'timestamp_x': 'timestamp',
            'description_x': 'json_server',
            'description_y': 'json_client'
        },
        inplace=True
    )
    df.drop(['timestamp_y'], axis=1, inplace=True)
    df = removed_cheaters('cheaters.db', df)
    sql_connection = sqlite3.connect('result.db')
    df.to_sql('result', sql_connection, if_exists='replace')
    print(df.info())

def display_memory_usage(snapshot, key_type='lineno'):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))

if __name__ == '__main__':
    tracemalloc.start()
    main()
    snapshot = tracemalloc.take_snapshot()
    display_memory_usage(snapshot)
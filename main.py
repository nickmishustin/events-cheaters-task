import pandas as pd
from datetime import date, datetime

def get_target_df(target_date, path):
    df= pd.read_csv(path, delimiter=',', header=0)
    df['date'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x).date())
    df = df[df['date'] == target_date]
    return df


target_date = date(year=2021, month=3, day=7)
df_server = get_target_df(target_date, 'server.csv')
df_client = get_target_df(target_date, 'client.csv')

print(df_server.head(), df_client.head())
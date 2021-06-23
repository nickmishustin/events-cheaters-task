from datetime import date, datetime
import pandas as pd
import sqlite3


class EventsWithoutCheaters:

    df = pd.DataFrame()

    def __init__(
        self,
        cheaters_path,
        result_path,
        server_path,
        client_path,
        target_date
    ):
        self.cheaters_path = cheaters_path
        self.result_path = result_path
        self.target_date = target_date
        self.server_path = server_path
        self.client_path = client_path

    def __get_target_df(self, path):
        df = pd.read_csv(path, delimiter=',', header=0)
        df['date'] = df['timestamp'].apply(
            lambda T: datetime.fromtimestamp(T).date()
        )
        df = df[df['date'] == self.target_date]
        df.drop('date', axis=1, inplace=True)
        return df

    def __remove_cheaters(self):
        sql_connection = sqlite3.connect(self.cheaters_path)
        df_cheaters = pd.read_sql_query(
            'SELECT * FROM cheaters',
            sql_connection
        )
        self.df = df_cheaters.merge(
            self.df, left_on='player_id',
            right_on='player_id'
        )
        sql_connection.close()
        self.df['ban_time'] = self.df['ban_time'].apply(
            lambda T: datetime.strptime(T, '%Y-%m-%d %H:%M:%S')
        )
        self.df['server_time'] = self.df['timestamp'].apply(
            lambda T: datetime.fromtimestamp(T)
        )
        self.df['datetime_diff'] = (
            self.df['ban_time'] - self.df['server_time']
        ).dt.days
        self.df = self.df[self.df['datetime_diff'] >= 1]
        self.df.drop(
            ['ban_time', 'server_time', 'datetime_diff'],
            axis=1,
            inplace=True
        )

    def set_df(self):
        df_server = self.__get_target_df(self.server_path)
        df_client = self.__get_target_df(self.client_path)
        self.df = df_server.merge(
            df_client,
            left_on='error_id',
            right_on='error_id'
        )
        self.df.rename(
            columns={
                'timestamp_x': 'timestamp',
                'description_x': 'json_server',
                'description_y': 'json_client'
            },
            inplace=True
        )
        self.df.drop(['timestamp_y'], axis=1, inplace=True)
        self.__remove_cheaters()

    def write_to_db(self):
        sql_connection = sqlite3.connect(self.result_path)
        self.df.to_sql('result', sql_connection, if_exists='replace')

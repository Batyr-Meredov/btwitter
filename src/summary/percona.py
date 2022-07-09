import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine


class Percona:
    def __init__(self):
        self.__connect()
        self.engine = create_engine('mysql+mysqlconnector://user:password@localhost:3306/twitter')

    def __connect(self):
        try:
            self.cnx = mysql.connector.connect(user="user", password="password", database='twitter')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def write(self, df, info):
        print(f'df: {df.shape[0]}')
        print(f'info: {info}')
        df.to_sql(con=self.engine, name='ner_2021_09', if_exists='append', index=False)
import psycopg2
import json
import yaml
import geopandas
from sqlalchemy import create_engine


class Db:

    def __init__(
            self,
            user,
            password,
            host,
            port,
            database
        ):

        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.conn_text = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.conn = psycopg2.connect(self.conn_text)
        self.cursor = self.conn.cursor()

    
    def create_table(self):

        f = open('config.json')
        data = json.load(f)
        query = 'CREATE TABLE ' + data['table_name'] +' ( '
        for i in range(len(data['columns'])):
            if i != 0:
                query = query + ' , '
            query = query + data['columns'][i]['column_name'] + ' ' + data['columns'][i]['datatype']
        query = query + ' );'
        
        self.cursor.execute(query)
        self.conn.commit()


    def read_table(
            self
        ):

        #Retrieving data
        self.cursor.execute('SELECT * FROM t1')

        #Fetching 1st row from the table
        result = self.cursor.fetchone();
        print(result)

        #Fetching 1st row from the table
        result = self.cursor.fetchall();
        print(result)

        #Commit your changes in the database
        self.conn.commit()


    def insert_in_table(
            self,
            data
        ):
        
        query = 'INSERT INTO t1 ( '
        for i in range(len(data['values'])):
            if i != 0:
                query = query + ' , '
            query = query + data['values'][i]['column_name']

        query = query + ') VALUES ( '

        for i in range(len(data['values'])):
            if i != 0:
                query = query + ' , '
            query = query + data['values'][i]['value']

        query = query + ');'

        self.cursor.execute(query)

        #Commit your changes in the database
        self.conn.commit()


    def delete_from_table(
            self
        ):

        table = input('Give the table name from which you want to delete : ')
        condition = input('Give the condition of deletion : ')
        #Retrieving data
        self.cursor.execute('DELETE FROM ' + table + ' WHERE ' + condition)

        #Commit your changes in the database
        self.conn.commit()


    def __del__(
            self
        ):
        self.conn.close()


def yaml_loader(
        filepath
    ):
    with open(filepath,"r") as file_descriptor:
        data = yaml.safe_load(file_descriptor)
    return data


if __name__ == '__main__':
    filepath = 'db_credentials.yaml'
    db_cred = yaml_loader(filepath)
    db = Db(db_cred['DB_USER'], db_cred['DB_PASSWORD'], db_cred['DB_HOST'], db_cred['DB_PORT'], db_cred['DB_DATABASE'])
    # db.create_table()
    f = open('queries.json')
    queries = json.load(f)

    for query in queries:
        if query['type'] == 'insert' :
            db.insert_in_table(query)
        elif query['type'] == 'read' :
            db.read_table()
        else :
            db.delete_from_table(query)
    
    db.read_table()

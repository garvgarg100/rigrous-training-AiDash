import psycopg2
from sqlalchemy import create_engine
import geopandas


def createTable(conn_text):
    engine = create_engine(conn_text)
    #Read shapefile using GeoPandas
    gdf = geopandas.read_file("shapefile_data/357/357_polygons.shp")
    
    #Import shapefile to databse
    gdf.to_postgis(name="tut4", con=engine, schema="public")
    print("success")


def readTable(conn_text):
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute('''SELECT * from tut1''')

    #Fetching 1st row from the table
    result = cursor.fetchone();
    print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall();
    print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

def updateTable(conn_text):
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(''' update tut1 set l3_code = 'g3' where l3_code = 'g4' ''')

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

def deleteFromTable(conn_text):
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(''' delete from tut1 where l3_code = 'g3' ''')

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

def customQuery(conn_text):
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    query = input()
    cursor.execute(query)

    conn.commit()
    conn.close()

user = "postgres"
password = "admin"
host = "localhost"
port = 5432
database = "postgres"

try:
    conn_text = f"postgresql://{user}:{password}@{host}:{port}/{database}"
except Exception as error:
    print(error)

# createTable(conn_text)
# readTable(conn_text)
# updateTable(conn_text)
# readTable(conn_text)
# deleteTable(conn_text)
# deleteFromTable(conn_text)

customQuery(conn_text)
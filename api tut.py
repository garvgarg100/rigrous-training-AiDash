import psycopg2
from sqlalchemy import create_engine
import geopandas

from flask import Flask, jsonify
from flask import Flask
app = Flask(__name__)



user = "postgres"
password = "admin"
host = "localhost"
port = 5432
database = "postgres"
conn_text = f"postgresql://{user}:{password}@{host}:{port}/{database}"

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/create")
def createTable():
    engine = create_engine(conn_text)
    #Read shapefile using GeoPandas
    gdf = geopandas.read_file("shapefile_data/357/357_polygons.shp")
    
    #Import shapefile to databse
    gdf.to_postgis(name="tut5", con=engine, schema="public")
    return "Table is created"


@app.route("/read")
def readTable():
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
    return result
    
@app.route("/update")
def updateTable():
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(''' update tut1 set l3_code = 'g4' where l3_code = 'w1' ''')

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()
    return "updation done successfully"

@app.route("/delete")
def deleteFromTable():
    conn = psycopg2.connect(conn_text)
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(''' delete from tut1 where l3_code = 'g4' ''')

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()
    return "Deletion done successfully"



if __name__=="__main__":
    app.run(debug=True)
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

load_dotenv()

########################################
# Update connection string information #
########################################
host = os.getenv("host")
user = os.getenv("user")
password = os.getenv("password")
sslmode = os.getenv("sslmode")

dbname = "postgres"

#----------------------------------------------------------------
# Create a new DB
conn_string = f"host={host} user={user} dbname={dbname} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
print("Connected to default database (postgres)")

cursor = conn.cursor()
cursor.execute("DROP DATABASE IF EXISTS dwh_analytics;")
cursor.execute("CREATE DATABASE dwh_analytics;")
print("Database dwh_analytics created.")

cursor.close()
conn.close()
#----------------------------------------------------------------
#Connect to the database
dbname2="dwh_analytics"
conn_string = f"host={host} user={user} dbname={dbname2} password={password} sslmode={sslmode}"
conn = psycopg2.connect(conn_string)
print("Connected to dwh_analytics")
cursor = conn.cursor()
cursor = conn.cursor()
#----------------------------------------------------------------

# Helper functions
def drop_recreate(c, tablename, create):
    c.execute("DROP TABLE IF EXISTS {0};".format(tablename))
    c.execute(create)
    print("Finished creating table {0}".format(tablename))

def populate_table(c, filename, tablename):
    f = open(filename, 'r')
    try:
        cursor.copy_from(f, tablename, sep=",", null = "")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    print("Finished populating {0}".format(tablename))

# Create Rider table
table = "rider"
filename = './data/riders.csv'
create = "CREATE TABLE rider (rider_id INTEGER PRIMARY KEY, first VARCHAR(50), last VARCHAR(50), address VARCHAR(100), birthday DATE, account_start_date DATE, account_end_date DATE, is_member BOOLEAN);"

drop_recreate(cursor, table, create)
populate_table(cursor, filename, table)

# Create Payment table
table = "payment"
filename = './data/payments.csv'
create = "CREATE TABLE payment (payment_id INTEGER PRIMARY KEY, date DATE, amount MONEY, rider_id INTEGER);"

drop_recreate(cursor, table, create)
populate_table(cursor, filename, table)

# Create Station table
table = "station"
filename = './data/stations.csv'
create = "CREATE TABLE station (station_id VARCHAR(50) PRIMARY KEY, name VARCHAR(75), latitude FLOAT, longitude FLOAT);"

drop_recreate(cursor, table, create)
populate_table(cursor, filename, table)

# Create Trip table
table = "trip"
filename = './data/trips.csv'
create = "CREATE TABLE trip (trip_id VARCHAR(50) PRIMARY KEY, rideable_type VARCHAR(75), start_at TIMESTAMP, ended_at TIMESTAMP, start_station_id VARCHAR(50), end_station_id VARCHAR(50), rider_id INTEGER);"

drop_recreate(cursor, table, create)
populate_table(cursor, filename, table)

# Clean up
conn.commit()
cursor.close()
conn.close()

print("All done!")
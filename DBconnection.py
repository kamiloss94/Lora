import psycopg2
import sys

# Connection parameters
param_dic = {
    "host"      : "192.168.1.11",
    "port"      : "5432",
    "database"  : "chirpstack_as_events",
    "user"      : "chirpstack_as_events",
    "password"  : "dbpassword"
}

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn

conn = connect(param_dic)

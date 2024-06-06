import psycopg2
from psycopg2 import OperationalError

def test_rds_conn():
    # db_alchemy_driver = "postgresql+psycopg2"
    # username = "pgde300"
    # password = "de300spring2024"
    # port = "5432"
    # endpoint = "billyin-hw4-pub-acc-2.cvwhaidrmrqj.us-east-2.rds.amazonaws.com"
    # default_db = "heart_disease_master"
    # CONN_URI = f"{db_alchemy_driver}://{username}:{password}@{endpoint}:{port}/{default_db}"

    # print("CONN_URI: ", CONN_URI)

    # # Create a connection to the RDS
    # engine = create_engine(CONN_URI)
    # conn = engine.connect()

    # print("Testing the RDS connection finished")

    # Connection parameters
    conn = None
    try:
        # Connection parameters
        print("Starting connection")
        conn = psycopg2.connect(
            host="billyin-hw4-pub-acc-3.cvwhaidrmrqj.us-east-2.rds.amazonaws.com",
            database="heart_disease_master",
            user="pgde300",
            password="de300spring2024",
            port="5432"
        )
        print("connection ended")

        # Create a cursor object
        cur = conn.cursor()
        
        # Print PostgreSQL version
        cur.execute('SELECT version();')
        db_version = cur.fetchone()
        print("You are connected to - ", db_version)
        
        # Close the communication with the PostgreSQL
        cur.close()
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

    print("worked")

test_rds_conn()
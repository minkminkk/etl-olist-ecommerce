import psycopg2 as pg
import psycopg2.sql as sql
import os
import oltp_schema as oltp


def create_oltp_schema():
    """
    Creates the OLTP schema for e-commerce pipeline in 
    olist-ecommerce PostgreSQL database
    """
    try:
        conn = pg.connect(
            database = "olist-ecommerce",
            user = "postgres",
            password = "1234"
        )
        conn.autocommit = True
        cur = conn.cursor()
        with open(os.path.join(os.path.dirname(__file__), 'schemas/oltp_schema.sql'), 'r') as script:
            script = script.read()
            cur.execute(script)
            # TODO: Execute commands in 'script' variable
        # cur.execute(oltp.create_tbl_orders)
    except:
        print("Error occurred")
    finally:
        conn.close()




if __name__ == '__main__':
    create_oltp_schema()
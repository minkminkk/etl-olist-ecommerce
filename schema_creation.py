import psycopg2 as pg
import psycopg2.sql as sql
import os


def create_oltp_schema():
    """
    Creates the OLTP schema for e-commerce pipeline in 
    olist-ecommerce PostgreSQL database
    
    Input file(s): create_oltp_schema.sql
    """
    conn = pg.connect(
        database = "olist-ecommerce",
        user = "postgres",
        password = "1234"
    )
    conn.autocommit = True
    cur = conn.cursor()

    with open(os.path.join(os.path.dirname(__file__), 'schemas/create_oltp_schema.sql'), 'r') as script:
        script = script.read()
        # TODO: Execute commands in 'script' variable

    conn.close()


if __name__ == '__main__':
    create_oltp_schema()
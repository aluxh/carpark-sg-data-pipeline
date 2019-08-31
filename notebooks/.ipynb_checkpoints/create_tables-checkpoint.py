import configparser
import psycopg2
from sql_stmt_create_tables import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print("Setting up Tables in Redshift")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Dropping Tables in Redshift")
    drop_tables(cur, conn)
    print("Creating Tables in Redshift")
    create_tables(cur, conn)

    conn.close()
    print("Tables are created in Redshift")


if __name__ == "__main__":
    main()
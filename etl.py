import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''Connects and loads the staging tables from s3'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''Inserts the information in the star schema datatables based on the sql queries in sql_queries.py'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
     '''This function is responsible for extracting the data from S3, transforming the data using staging tables, and loading the 
         data into the appropriate tables'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, result_queries, copy, insert, result
from time import time

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Executing {}".format(copy[query]))
        start = time()

        cur.execute(query)
        conn.commit()

        print("Finished in {0:.4f} sec".format(time()-start))
    print("-------------------------------------------------------------------------------------------")

def insert_tables(cur, conn):
    for query in insert_table_queries:
        start = time()
        print("Executing {}".format(insert[query]))

        cur.execute(query)
        conn.commit()

        print("Finished in  {0:.4f} sec".format(time()-start))
    print("-------------------------------------------------------------------------------------------")

def print_results(cur, conn):
    for query in result_queries:
        cur.execute(query)
        count = cur.fetchone()[0]
        conn.commit()

        print("Table {} has \t {} records".format(result[query], count))
    print("-------------------------------------------------------------------------------------------")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    # Print results after the ETL finishes
    print_results(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
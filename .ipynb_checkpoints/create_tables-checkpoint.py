import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

        print("Executed successfully {}".format(query))
    print("-------------------------------------------------------------------------------------------")

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        print("Executed successfully {}".format(create[query]))
    print("-------------------------------------------------------------------------------------------")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
import os
import sys
import psycopg2
import configparser

env_path = os.path.join(os.path.expanduser('~'), 'env')

def get_db_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(env_path, 'db_config.ini'))
    return config

def main(table_name):
    db_config = get_db_config()
    financial_db_info= db_config['prod']
    conn = psycopg2.connect(f"dbname='{financial_db_info['dbname']}' user='{financial_db_info['user']}' host='{financial_db_info['host']}' password='{financial_db_info['password']}'")
    cur = conn.cursor()
    print('Connecting to Database')
    sql = f"COPY {table_name} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open(f"/home/ppp1987524/TFE-data/financialdata/pg_extract/data/{table_name}.csv", "w") as file:
        cur.copy_expert(sql, file)
        cur.close()
    print('CSV File has been created')

if __name__ == "__main__":
    table_name = sys.argv[1]
    main(table_name)

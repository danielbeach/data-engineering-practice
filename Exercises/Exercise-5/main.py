import psycopg2
import os
import glob
import pandas as pd


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    create_sql_from_csv_files_in_directory('./data',
                                           psycopg2.connect(host=host, database=database, user=user, password=pas))


def generate_create_table_sql(df, table_name):

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column in df.columns:
        data_type = df[column].dtype
        sql_type = "TEXT" if data_type == 'object' else "INTEGER"
        create_table_sql += f"{column} {sql_type}, "

    create_table_sql = create_table_sql.rstrip(', ') + ");"
    return create_table_sql


def create_sql_from_csv_files_in_directory(directory, conn):
    if not os.path.exists(directory):
        raise FileNotFoundError("Directory not found")
    csv_files = glob.glob(os.path.join(directory, '**', '*.csv'), recursive=True)

    with open('ddl.sql', 'w') as sql_file:
        sql_file.write("ALTER ROLE postgres WITH PASSWORD \'postgres\';" + '\n')

    for filename in csv_files:

        csv_file = os.path.join(filename)
        table_name = os.path.split(os.path.splitext(filename)[0])[1]

        df = pd.read_csv(csv_file)

        create_table_sql = generate_create_table_sql(df, table_name)

        with open('ddl.sql', 'a') as sql_file:
            sql_file.write(create_table_sql + '\n')
            sql_file.write('\n-- INSERT DATA --\n')
            for _, row in df.iterrows():
                values = ', '.join([f"'{str(value)}'" for value in row])
                sql_file.write(f"INSERT INTO {table_name} VALUES ({values});\n")

    cursor = conn.cursor()
    with open("ddl.sql", "r") as sql_file:
        sql_queries = sql_file.read()
        cursor.execute(sql_queries)
        conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()

import os
import psycopg2
from sql.create_table import AccountTable
from sql.create_table import ProductTable
from sql.create_table import TransactionTable
from sql.create_index import AccountIndex
from sql.create_index import TransactionIndex
from sql.create_index import ProductIndex


# this function create new tables
def create_table(connection):
    cursor = connection.cursor()
    cursor.execute(AccountTable)
    cursor.execute(ProductTable)
    cursor.execute(TransactionTable)
    connection.commit()


# this function create index on each table
def create_index(connection):
    cursor = connection.cursor()
    cursor.execute(AccountIndex)
    cursor.execute(TransactionIndex)
    cursor.execute(ProductIndex)
    connection.commit()


# import data into the table in our postgres database
def import_data(connection, ):
    cursor = connection.cursor()

    for files in os.listdir("data"):
        table_name = files.split(".")[0]

        # insert csv data into postgres table
        with open("data/" + files, "r") as f:
            next(f)  # skip header

            cursor.copy_from(f, table_name, sep=",") # copy from csv files  to ann existing table
            connection.commit()


def main():
    host = 'postgres'
    database = 'postgres'
    user = 'postgres'
    pas = 'postgre'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

    # create all tables
    create_table(conn)

    # create all index
    create_index(conn)

    # insert data into tables
    import_data(conn)


if __name__ == '__main__':
    main()

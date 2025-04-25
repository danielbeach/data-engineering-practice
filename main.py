import psycopg2
import csv

conn = psycopg2.connect("host=postgres dbname=postgres user=postgres password=postgres")
cur = conn.cursor()

# Run schema
with open("schema.sql", "r") as f:
    cur.execute(f.read())
conn.commit()

# Insert data from accounts.csv
with open("data/accounts.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    for row in reader:
        cur.execute("""
            INSERT INTO accounts (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)
conn.commit()

# Insert data from products.csv
with open("data/products.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cur.execute("""
            INSERT INTO products (product_id, product_code, product_description)
            VALUES (%s, %s, %s)
        """, row)
conn.commit()

with open('data/transactions.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        try:
            transaction_date = row[1].strip()
            product_id = int(row[2].strip())
            quantity = int(row[5].strip())
            customer_id = int(row[6].strip())
            price = 19.99 if product_id == 345 else 29.99  # Gán giá tạm nếu chưa có cột price

            cur.execute("""
                INSERT INTO transactions (customer_id, product_id, transaction_date, quantity, price)
                VALUES (%s, %s, %s, %s, %s)
            """, (customer_id, product_id, transaction_date, quantity, price))
        except Exception as e:
            print(f"Lỗi khi xử lý dòng {row}: {e}")

print("Dữ liệu đã được import thành công.")
print("\n🧾 Dữ liệu bảng accounts:")
cur.execute("SELECT * FROM accounts")
for row in cur.fetchall():
    print(row)

# In dữ liệu trong bảng products
print("\n📦 Dữ liệu bảng products:")
cur.execute("SELECT * FROM products")
for row in cur.fetchall():
    print(row)

# In dữ liệu trong bảng transactions
print("\n💳 Dữ liệu bảng transactions:")
cur.execute("SELECT * FROM transactions")
for row in cur.fetchall():
    print(row)

print("\n✅ Dữ liệu đã được import và in ra thành công.")
cur.close()
conn.close()
import psycopg2
import csv

conn = psycopg2.connect("host=postgres dbname=postgres user=postgres password=postgres")
cur = conn.cursor()

# Run schema
with open("schema.sql", "r") as f:
    cur.execute(f.read())
conn.commit()

# Insert data from accounts.csv
with open("data/accounts.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    for row in reader:
        cur.execute("""
            INSERT INTO accounts (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, row)
conn.commit()

# Insert data from products.csv
with open("data/products.csv", "r") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        cur.execute("""
            INSERT INTO products (product_id, product_code, product_description)
            VALUES (%s, %s, %s)
        """, row)
conn.commit()

with open('data/transactions.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        try:
            transaction_date = row[1].strip()
            product_id = int(row[2].strip())
            quantity = int(row[5].strip())
            customer_id = int(row[6].strip())
            price = 19.99 if product_id == 345 else 29.99  # Gán giá tạm nếu chưa có cột price

            cur.execute("""
                INSERT INTO transactions (customer_id, product_id, transaction_date, quantity, price)
                VALUES (%s, %s, %s, %s, %s)
            """, (customer_id, product_id, transaction_date, quantity, price))
        except Exception as e:
            print(f"Lỗi khi xử lý dòng {row}: {e}")

print("Dữ liệu đã được import thành công.")
print("\n🧾 Dữ liệu bảng accounts:")
cur.execute("SELECT * FROM accounts")
for row in cur.fetchall():
    print(row)

# In dữ liệu trong bảng products
print("\n📦 Dữ liệu bảng products:")
cur.execute("SELECT * FROM products")
for row in cur.fetchall():
    print(row)

# In dữ liệu trong bảng transactions
print("\n💳 Dữ liệu bảng transactions:")
cur.execute("SELECT * FROM transactions")
for row in cur.fetchall():
    print(row)

print("\n✅ Dữ liệu đã được import và in ra thành công.")
cur.close()
conn.close()

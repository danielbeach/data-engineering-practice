import os
import csv
import psycopg2
import re

# Database connection parameters
DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": "5432"
}

def clean_column_name(name):
    """Convert column names to valid PostgreSQL identifiers"""
    # Replace spaces and special characters with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name.lower().strip())
    # Ensure name doesn't start with a digit
    if clean_name[0].isdigit():
        clean_name = 'col_' + clean_name
    return clean_name

def infer_column_type(values):
    """Infer column data type based on values"""
    # Check if all values can be integers
    if all(val.isdigit() or val == '' for val in values):
        return "INTEGER"
    
    # Check if all values can be floats
    try:
        for val in values:
            if val and not val.isspace():
                float(val)
        return "NUMERIC"
    except ValueError:
        pass
    
    # Check for date-like patterns (simplified check)
    date_patterns = [
        r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
        r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
        r'^\d{2}-\d{2}-\d{4}$'   # DD-MM-YYYY
    ]
    
    if all(any(re.match(pattern, val) for pattern in date_patterns) or val == '' for val in values if val):
        return "DATE"
    
    # Default to text
    return "TEXT"

def analyze_csv_structure(file_path):
    """Analyze CSV file structure and return column names and types"""
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        
        # Clean header names
        clean_headers = [clean_column_name(header) for header in headers]
        
        # Read sample data to infer column types
        sample_data = []
        for _ in range(100):  # Read up to 100 rows as samples
            try:
                row = next(csv_reader)
                sample_data.append(row)
            except StopIteration:
                break
        
        # Transpose sample data
        column_samples = [[] for _ in range(len(clean_headers))]
        for row in sample_data:
            for i, value in enumerate(row[:len(clean_headers)]):
                column_samples[i].append(value)
        
        # Infer column types
        column_types = [infer_column_type(samples) for samples in column_samples]
        
        return clean_headers, column_types

def generate_create_table_statement(table_name, columns, column_types):
    """Generate SQL CREATE TABLE statement with primary key and indexes"""
    # Assume first column is the primary key
    pk_column = columns[0]
    
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    
    # Add columns with their types
    for i, (col, col_type) in enumerate(zip(columns, column_types)):
        sql += f"    {col} {col_type}"
        if col == pk_column:
            sql += " PRIMARY KEY"
        if i < len(columns) - 1:
            sql += ",\n"
    
    sql += "\n);"
    
    # Create indexes on all columns (excluding primary key)
    indexes = []
    for col in columns:
        if col != pk_column:
            idx_name = f"idx_{table_name}_{col}"
            idx_sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({col});"
            indexes.append(idx_sql)
    
    return sql, indexes

def create_all_tables(conn, cur):
    """Create tables for all CSV files in the data directory"""
    data_dir = "./data"
    create_statements = []
    foreign_key_statements = []
    
    # First pass: Generate CREATE TABLE statements
    for file_name in os.listdir(data_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            table_name = os.path.splitext(file_name)[0].lower()
            
            columns, column_types = analyze_csv_structure(file_path)
            create_sql, indexes = generate_create_table_statement(table_name, columns, column_types)
            
            print(f"Creating table for {file_name}...")
            cur.execute(create_sql)
            
            # Create indexes
            for index_sql in indexes:
                cur.execute(index_sql)
            
            create_statements.append((table_name, create_sql, indexes))
    
    # Second pass: Try to identify and add foreign key relationships
    # This is a simplified approach - in real scenarios, you'd need more sophisticated analysis
    for table_name1, _, _ in create_statements:
        for table_name2, _, _ in create_statements:
            if table_name1 != table_name2:
                # Check if table1 has a column matching table2's name (or with _id suffix)
                fk_column = f"{table_name2}_id"
                
                # Check if this column exists in table1
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name1}' AND column_name = '{fk_column}'")
                if cur.fetchone():
                    # Add foreign key constraint
                    fk_sql = f"""
                    ALTER TABLE {table_name1} 
                    ADD CONSTRAINT fk_{table_name1}_{table_name2} 
                    FOREIGN KEY ({fk_column}) REFERENCES {table_name2} (id);
                    """
                    try:
                        cur.execute(fk_sql)
                        foreign_key_statements.append(fk_sql)
                        print(f"Added foreign key from {table_name1}.{fk_column} to {table_name2}.id")
                    except psycopg2.Error as e:
                        print(f"Could not add foreign key: {e}")
    
    conn.commit()
    return create_statements, foreign_key_statements

def import_csv_data(conn, cur):
    """Import data from CSV files into the created tables"""
    data_dir = "./data"
    
    for file_name in os.listdir(data_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            table_name = os.path.splitext(file_name)[0].lower()
            
            with open(file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file)
                headers = next(csv_reader)
                clean_headers = [clean_column_name(header) for header in headers]
                
                # Get column names from the database
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                db_columns = [row[0] for row in cur.fetchall()]
                
                # Match the CSV columns with database columns
                valid_columns = [col for col in clean_headers if col in db_columns]
                
                for row in csv_reader:
                    # Map row values to valid columns
                    values = [row[clean_headers.index(col)] if clean_headers.index(col) < len(row) else None for col in valid_columns]
                    
                    # Create placeholders for SQL
                    placeholders = ", ".join(["%s"] * len(valid_columns))
                    columns_str = ", ".join(valid_columns)
                    
                    # Insert row
                    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    try:
                        cur.execute(insert_sql, values)
                    except psycopg2.Error as e:
                        print(f"Error inserting row into {table_name}: {e}")
                        conn.rollback()
                        continue
            
            conn.commit()
            print(f"Imported data from {file_name} into {table_name}")

def main():
    print("Starting data import process...")
    
    # Connect to the database
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        print("Connected to PostgreSQL database successfully")
        
        # Create tables
        create_statements, foreign_key_statements = create_all_tables(conn, cur)
        
        # Import data
        import_csv_data(conn, cur)
        
        print("Data import completed successfully")
        
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
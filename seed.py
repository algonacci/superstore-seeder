import pandas as pd
import psycopg2

DB_NAME = "superstore"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

CSV_FILE_PATH = "superstore.csv"

def create_connection():
    try:
        connection = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def create_superstore_table(connection):
    try:
        cursor = connection.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS superstore (
                order_id INTEGER,
                order_date DATE,
                ship_date DATE,
                ship_mode VARCHAR(50),
                customer_id VARCHAR(50),
                customer_name VARCHAR(255),
                segment VARCHAR(50),
                country VARCHAR(100),
                city VARCHAR(100),
                state VARCHAR(100),
                postal_code VARCHAR(20),
                region VARCHAR(50),
                product_id VARCHAR(50),
                category VARCHAR(50),
                sub_category VARCHAR(50),
                product_name VARCHAR(255),
                sales NUMERIC,
                quantity INTEGER,
                discount NUMERIC,
                profit NUMERIC
            );
        """

        cursor.execute(create_table_query)

        connection.commit()

        print("Table created successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while creating the table:", error)

def seed_superstore_data(connection):
    try:
        cursor = connection.cursor()

        df = pd.read_csv(CSV_FILE_PATH, encoding="latin-1")

        for index, row in df.iterrows():
            query = """
                INSERT INTO superstore (
                    order_id, order_date, ship_date, ship_mode,
                    customer_id, customer_name, segment, country,
                    city, state, postal_code, region, product_id,
                    category, sub_category, product_name,
                    sales, quantity, discount, profit
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """
            values = (
                row['Order ID'], row['Order Date'], row['Ship Date'], row['Ship Mode'],
                row['Customer ID'], row['Customer Name'], row['Segment'], row['Country'],
                row['City'], row['State'], str(row['Postal Code']) if not pd.isnull(row['Postal Code']) else '',
                row['Region'], row['Product ID'], row['Category'], row['Sub-Category'],
                row['Product Name'], row['Sales'], row['Quantity'], row['Discount'], row['Profit']
            )
            cursor.execute(query, values)

        connection.commit()

        print("Data seeded successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while seeding the data:", error)

if __name__ == "__main__":
    connection = create_connection()
    if connection:
        create_superstore_table(connection)
        seed_superstore_data(connection)
        connection.close()


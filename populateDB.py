import csv
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()

# Read data from CSV 0
csv_0_path = 'data/shipping_data_0.csv'
with open(csv_0_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        cursor.execute('''
            INSERT INTO table_name_1 (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (row['origin_warehouse'], row['destination_store'], row['product'], row['on_time'], row['product_quantity'], row['driver_identifier']))

csv_1_path = 'data/shipping_data_1.csv'
with open(csv_1_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        product_name = row['product_name']  
        quantity = row['quantity']          
        origin = row['origin']              
        destination = row['destination']   
        cursor.execute('''
            INSERT INTO table_name_1 (product, product_quantity, origin_warehouse, destination_store)
            VALUES (?, ?, ?, ?)
        ''', (product_name, quantity, origin, destination))

csv_2_path = 'data/shipping_data_2.csv'
with open(csv_2_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        product_name = row['product_name']  
        quantity = row['quantity']          
        origin = row['origin']             
        destination = row['destination']    
        
        cursor.execute('''
            INSERT INTO table_name_1 (product, product_quantity, origin_warehouse, destination_store)
            VALUES (?, ?, ?, ?)
        ''', (product_name, quantity, origin, destination))

# Commit changes and close the connection
conn.commit()
conn.close()

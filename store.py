from flask import Flask, request, jsonify, send_file
import csv
import os
import uuid
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv
from io import StringIO

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Database connection parameters
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

# Function to establish database connection
def get_db_connection():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

# Function to initialize database schema
def initialize_database():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_status (
                    store_id VARCHAR(255),
                    timestamp_utc VARCHAR(255),
                    status VARCHAR(50)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_hours (
                    store_id VARCHAR(255),
                    day_of_week INT,
                    start_time_local TIME,
                    end_time_local TIME
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS store_timezone (
                    store_id VARCHAR(255),
                    timezone_str VARCHAR(255)
                )
            """)
            conn.commit()

# Function to process CSV data and store in the database
def process_csv_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Process store status CSV
            with open('store_status.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    cur.execute("INSERT INTO store_status (store_id, timestamp_utc, status) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
            
            # Process store hours CSV
            with open('store_hours.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    cur.execute("INSERT INTO store_hours (store_id, day_of_week, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)", (row[0], row[1], row[2], row[3]))
            
            # Process store timezone CSV
            with open('store_timezone.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                for row in reader:
                    cur.execute("INSERT INTO store_timezone (store_id, timezone_str) VALUES (%s, %s)", (row[0], row[1]))
            
            conn.commit()


# Endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    try:
        # Trigger report generation process
        process_csv_data()
        
        # Generate a random report ID
        report_id = str(uuid.uuid4())
        
        return jsonify({'report_id': report_id}), 200
    except Exception as e:
        return str(e), 500

# Endpoint to get report status or CSV
@app.route('/get_report', methods=['GET'])
def get_report():
    try:
        report_id = request.args.get('report_id')
        
        # Check if report generation is complete (You need to implement this logic)
        is_report_complete = True  # Example implementation, you need to replace this with actual logic
        
        if is_report_complete:
            # If report generation is complete, return complete status along with CSV file
            csv_data = generate_report_csv()  # You need to implement this function to generate CSV data
            return send_file(csv_data, download_name='report.csv', as_attachment=True), 200
        else:
            # If report generation is still running, return running status
            return jsonify({'status': 'Running'}), 200
    except Exception as e:
        return str(e), 500

# Function to generate report CSV
def generate_report_csv():
    # Connect to the database
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Fetch data from the database
        cur.execute("SELECT * FROM store_status")
        store_status_data = cur.fetchall()

        # Calculate report data
        report_data = []
        for row in store_status_data:
            # Your logic to process store status data and calculate report goes here
            # For demonstration, let's assume we are fetching store_id, timestamp_utc, and status from the database
            store_id, timestamp_utc, status = row
            report_row = [store_id, timestamp_utc, status]  # Replace with actual report data
            report_data.append(report_row)

        # Generate CSV
        csv_output = StringIO()
        csv_writer = csv.writer(csv_output)
        csv_writer.writerow(['store_id', 'timestamp_utc', 'status'])  # Write header
        csv_writer.writerows(report_data)

        # Save CSV to a temporary file
        temp_file_path = '/tmp/report.csv'  # Change this path to your desired location
        with open(temp_file_path, 'w') as csv_file:
            csv_file.write(csv_output.getvalue())

        return temp_file_path
    finally:
        # Close database connection
        cur.close()
        conn.close()

if __name__ == '__main__':
    initialize_database()  # Initialize database schema
    app.run(debug=True)  # Run the Flask app

from flask import Flask, request, jsonify, send_file
import csv
import os
import uuid
import psycopg2
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

def get_db_connection():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

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

def process_csv_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            with open('store_status.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)  
                for row in reader:
                    cur.execute("INSERT INTO store_status (store_id, timestamp_utc, status) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
            
            with open('store_hours.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader) 
                for row in reader:
                    cur.execute("INSERT INTO store_hours (store_id, day_of_week, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)", (row[0], row[1], row[2], row[3]))
            
            with open('store_timezone.csv', 'r') as file:
                reader = csv.reader(file)
                next(reader)  
                for row in reader:
                    cur.execute("INSERT INTO store_timezone (store_id, timezone_str) VALUES (%s, %s)", (row[0], row[1]))
            
            conn.commit()

@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    try:
        process_csv_data()
        report_id = str(uuid.uuid4())
        return jsonify({'report_id': report_id}), 200
    except Exception as e:
        return str(e), 500

@app.route('/get_report', methods=['GET'])
def get_report():
    try:
        report_id = request.args.get('report_id')
        is_report_complete = True  
        
        if is_report_complete:
            csv_data = generate_report_csv()  
            return send_file(csv_data, download_name='report.csv', as_attachment=True), 200
        else:
            return jsonify({'status': 'Running'}), 200
    except Exception as e:
        return str(e), 500

def generate_report_csv():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT * FROM store_status")
        store_status_data = cur.fetchall()

        report_data = []
        for row in store_status_data:
            store_id, timestamp_utc, status = row
            report_row = [store_id, timestamp_utc, status]  
            report_data.append(report_row)

        csv_output = StringIO()
        csv_writer = csv.writer(csv_output)
        csv_writer.writerow(['store_id', 'timestamp_utc', 'status'])  
        csv_writer.writerows(report_data)

        temp_file_path = '/tmp/report.csv'  
        with open(temp_file_path, 'w') as csv_file:
            csv_file.write(csv_output.getvalue())

        return temp_file_path
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    initialize_database()  
    app.run(debug=True)  

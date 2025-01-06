import csv
import gzip
import io
import time
import requests
import json
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import MonthEnd, QuarterEnd
from dateutil.relativedelta import relativedelta

# Function to get access token
def get_access_token(client_id, client_secret, refresh_token):
    token_url = "https://api.amazon.com/auth/o2/token"
    token_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
    }
    
    token_response = requests.post(token_url, data=token_data, headers=token_headers)
    token_response.raise_for_status()
    return token_response.json()["access_token"]

client_id = ""
client_secret = ""
refresh_token = ""

# Get the access token
access_token = get_access_token(client_id, client_secret, refresh_token)

# Setup your endpoint and headers as needed
endpoint = "https://sellingpartnerapi-na.amazon.com"
headers = {
    "x-amz-access-token": access_token,
    "Content-Type": "application/json"
}

def get_start_end_dates_for_period(period, periods_ago=0):
    now = datetime.now()
    if period == 'WEEK':
        end_date = now - timedelta(days=(now.weekday() + 2) % 7 + periods_ago * 7)
        start_date = end_date - timedelta(days=6)
    elif period == 'MONTH':
        end_date = (now.replace(day=1) - relativedelta(months=periods_ago) + MonthEnd(0))
        start_date = end_date.replace(day=1)
    elif period == 'YEAR':
        end_date = datetime(now.year - periods_ago, 12, 31)
        start_date = datetime(now.year - periods_ago, 1, 1)
    elif period == 'YTD':
        end_date = now.replace(day=1) - relativedelta(months=1) + MonthEnd(0)
        start_date = datetime(now.year, 1, 1)
    elif period == 'LYTD':
        end_date = datetime(now.year - 1, now.month, 1) - relativedelta(months=1) + MonthEnd(0)
        start_date = datetime(now.year - 1, 1, 1)
    elif period == 'QUARTER':
        current_quarter = (now.month - 1) // 3 + 1
        current_quarter_start = pd.Period(year=now.year, quarter=current_quarter, freq='Q').start_time
        current_quarter_end = pd.Period(year=now.year, quarter=current_quarter, freq='Q').end_time
        start_date = current_quarter_start - relativedelta(months=3*periods_ago)
        end_date = current_quarter_end - relativedelta(months=3*periods_ago) + QuarterEnd(0)
    else:
        raise ValueError("Unsupported period for start date calculation")
    
    return start_date.strftime("%Y-%m-%dT00:00:00Z"), end_date.strftime("%Y-%m-%dT23:59:59Z")

os.makedirs('weeklyData/trafficData', exist_ok=True)

def get_report_definitions():
    this_week_start, this_week_end = get_start_end_dates_for_period('WEEK', 0)
    last_week_start, last_week_end = get_start_end_dates_for_period('WEEK', 1)

    this_week_start_dt = datetime.strptime(this_week_start, "%Y-%m-%dT00:00:00Z")
    this_week_end_dt = datetime.strptime(this_week_end, "%Y-%m-%dT23:59:59Z")

    last_year_week_start = (this_week_start_dt - relativedelta(years=1)).strftime("%Y-%m-%dT00:00:00Z")
    last_year_week_end = (this_week_end_dt - relativedelta(years=1)).strftime("%Y-%m-%dT23:59:59Z")
    
    print(f"this_week_start: {this_week_start}, this_week_end: {this_week_end}")
    print(f"last_year_week_start: {last_year_week_start}, last_year_week_end: {last_year_week_end}")
    
    return [
        {
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": last_week_start,
            "end_time": last_week_end,
            "fieldnames": ["trafficAggregate", "trafficByAsin", "startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "WEEK"
            },
            "report_name": "last_week"
        },
        {
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": this_week_start,
            "end_time": this_week_end,
            "fieldnames": ["trafficAggregate", "trafficByAsin", "startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "WEEK"
            },
            "report_name": "this_week"
        },
        {
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": last_year_week_start,
            "end_time": last_year_week_end,
            "fieldnames": ["trafficAggregate", "trafficByAsin", "startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "DAY"
            },
            "report_name": "this_week_last_year"
        }
    ]

def create_report(report_type, data_start_time, data_end_time, report_options):
    report_request_body = {
        "reportType": report_type,
        "dataStartTime": data_start_time,
        "dataEndTime": data_end_time,
        "marketplaceIds": ["ATVPDKIKX0DER"]
    }
    if report_options:
        report_request_body["reportOptions"] = report_options

    create_report_url = f"{endpoint}/reports/2021-06-30/reports"
    report_response = requests.post(create_report_url, headers=headers, data=json.dumps(report_request_body))
    
    if report_response.status_code in [200, 202]:
        return report_response.json().get("reportId")
    else:
        print(f"Error creating report: {report_response.json()}")
        return None

def check_report_status(report_id):
    report_status_url = f"{endpoint}/reports/2021-06-30/reports/{report_id}"
    response = requests.get(report_status_url, headers=headers)
    response.raise_for_status()
    return response.json()

def download_report(report_id, report_type, fieldnames, report_options, report_name):
    retries = 3
    for _ in range(retries):
        try:
            status = check_report_status(report_id)
            while status["processingStatus"] not in ["DONE", "CANCELLED", "FATAL"]:
                print(f"Report status for {report_id}: {status['processingStatus']}")
                time.sleep(30)  # Wait for 30 seconds before checking again
                status = check_report_status(report_id)

            if status["processingStatus"] == "DONE":
                report_document_id = status["reportDocumentId"]
                report_document_url = f"{endpoint}/reports/2021-06-30/documents/{report_document_id}"
                document_response = requests.get(report_document_url, headers=headers)
                document_response.raise_for_status()
                document_info = document_response.json()
                download_url = document_info["url"]
                compression_algorithm = document_info.get("compressionAlgorithm")

                report_content_response = requests.get(download_url)
                report_content_response.raise_for_status()

                if compression_algorithm == "GZIP":
                    with gzip.open(io.BytesIO(report_content_response.content), 'rt', encoding='utf-8') as gzipped_file:
                        report_content = gzipped_file.read()
                else:
                    report_content = report_content_response.content.decode('utf-8')

                report_json = json.loads(report_content)
                
                traffic_aggregate_data = report_json.get("trafficAggregate", [])
                traffic_by_asin_data = report_json.get("trafficByAsin", [])

                aggregate_csv_filename = f"weeklyData/trafficData/{report_name}_aggregate_report.csv"
                with open(aggregate_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in traffic_aggregate_data:
                        row = {field: record.get(field, None) for field in fieldnames}
                        writer.writerow(row)

                asin_csv_filename = f"weeklyData/trafficData/{report_name}_asin_report.csv"
                asin_fieldnames = list(traffic_by_asin_data[0].keys()) if traffic_by_asin_data else []
                with open(asin_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=asin_fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in traffic_by_asin_data:
                        writer.writerow(record)

                merge_asin_report(asin_csv_filename)

                print(f"Report {report_type} downloaded and saved successfully.")
            else:
                print(f"Report processing for {report_id} failed with status: {status['processingStatus']}")
            break
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Error occurred: {e}")
            time.sleep(10)  # Wait for 10 seconds before retrying
    else:
        print(f"Failed to download report {report_id} after {retries} retries.")

def delete_old_files():
    folder = 'weeklyData/trafficData'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink():
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def merge_asin_report(asin_csv_filename):
    asin_df = pd.read_csv(asin_csv_filename)
    merged_asin_df = asin_df.groupby('asin', as_index=False).agg({
        'glanceViews': 'sum'
    })
    merged_asin_df.to_csv(asin_csv_filename, index=False)

def add_category_to_asin_reports(category_lookup_path, report_files, output_dir):
    # Load the category lookup CSV
    category_lookup = pd.read_csv(category_lookup_path)
    
    # Create a dictionary for quick lookup
    asin_to_category = dict(zip(category_lookup['ASIN'], category_lookup['Category']))
    
    # Function to add category to each ASIN report and save the updated DataFrame
    def add_category_to_asin_report(file_path, asin_to_category, output_dir):
        # Read the ASIN report
        df = pd.read_csv(file_path)
        
        # Add the category column
        df['Category'] = df['asin'].map(asin_to_category).fillna('Other')
        
        # Get the file name from the path
        file_name = os.path.basename(file_path)
        
        # Save the updated file
        output_path = os.path.join(output_dir, file_name)
        df.to_csv(output_path, index=False)
        print(f"Updated {output_path}")
    
    # Process each ASIN report file
    for report_file in report_files:
        add_category_to_asin_report(report_file, asin_to_category, output_dir)
    
def fetch_and_process_reports():
    delete_old_files()
    reports_to_fetch = get_report_definitions()
    for report in reports_to_fetch:
        report_id = create_report(report["report_type"], report["start_time"], report["end_time"], report["report_options"])
        if report_id:
            download_report(report_id, report["report_type"], report["fieldnames"], report["report_options"], report["report_name"])

    # After downloading and processing reports, add categories to each ASIN report
    add_category_to_asin_reports(
        category_lookup_path='', 
        report_files=[
            'weeklyData/trafficData/last_week_asin_report.csv',
            'weeklyData/trafficData/this_week_asin_report.csv',
            'weeklyData/trafficData/this_week_last_year_asin_report.csv'
        ],
        output_dir='weeklyData/trafficData'
    )

def calculate_weekly_percentages():
    # Load the ASIN-to-category mapping file
    category_lookup = pd.read_csv('')
    asin_to_category = dict(zip(category_lookup['ASIN'], category_lookup['Category']))

    # Load the weekly data
    this_week = pd.read_csv("weeklyData/trafficData/this_week_asin_report.csv")
    last_week = pd.read_csv("weeklyData/trafficData/last_week_asin_report.csv")
    last_year_this_week = pd.read_csv("weeklyData/trafficData/this_week_last_year_asin_report.csv")

    # Print the column names of each dataframe for debugging
    print("Columns in 'this_week':", this_week.columns)
    print("Columns in 'last_week':", last_week.columns)
    print("Columns in 'last_year_this_week':", last_year_this_week.columns)

    # Add a 'Week' column to each DataFrame
    this_week['Week'] = 'This Week'
    last_week['Week'] = 'Last Week'
    last_year_this_week['Week'] = 'Last Year This Week'

    # Map ASINs to Categories in each DataFrame
    this_week['Category'] = this_week['asin'].map(asin_to_category).fillna('Other')
    last_week['Category'] = last_week['asin'].map(asin_to_category).fillna('Other')
    last_year_this_week['Category'] = last_year_this_week['asin'].map(asin_to_category).fillna('Other')

    # Check if 'glanceViews' exists in the data, adjust if necessary
    if 'glanceViews' not in this_week.columns or 'glanceViews' not in last_week.columns or 'glanceViews' not in last_year_this_week.columns:
        raise KeyError("The 'glanceViews' column is missing from the CSV files.")

    # Combine the DataFrames
    combined = pd.concat([this_week, last_week, last_year_this_week])

    # Convert 'glanceViews' to float
    combined['glanceViews'] = pd.to_numeric(combined['glanceViews'], errors='coerce')

    # Group by Category and Week, then sum the metrics
    grouped = combined.groupby(['Category', 'Week']).agg({
        'glanceViews': 'sum'
    }).reset_index()

    # Pivot the table to have weeks as columns
    pivoted = grouped.pivot(index='Category', columns='Week', values='glanceViews').reset_index()

    # Calculate week-over-week and year-over-year percentage change
    pivoted['WoW (Last Week vs This Week) % Glance Views'] = (
        (pivoted['This Week'] - pivoted['Last Week']) / pivoted['Last Week'] * 100
    ).fillna(0)  # Fill missing values with 0
    pivoted['YoY (Last Year This Week vs This Week) % Glance Views'] = (
        (pivoted['This Week'] - pivoted['Last Year This Week']) / pivoted['Last Year This Week'] * 100
    ).fillna(0)  # Fill missing values with 0

    # Modify the headers to include "Glance Views" at the end
    headers = ['Category', 'This Week Glance Views', 'Last Week Glance Views', 'Last Year This Week Glance Views', 
               'WoW (Last Week vs This Week) % Glance Views', 'YoY (Last Year This Week vs This Week) % Glance Views']

    # Save the result to a CSV file with updated headers
    pivoted.to_csv("weeklyData/trafficData/weekly_traffic_report.csv", index=False, header=headers)
    print("Weekly and Yearly percentages by category (glance views) have been calculated and saved.")
    
if __name__ == "__main__":
    fetch_and_process_reports()
    calculate_weekly_percentages()

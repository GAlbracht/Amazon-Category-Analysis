import csv
import glob
import gzip
import io
import time
import requests
import json
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
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

# Replace with your actual client_id, client_secret, and refresh_token
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

# Function to split a large date range into smaller chunks (if over 14 days)
def split_date_range(start_date, end_date, max_days=14):
    chunks = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks

# Get the start and end dates for the current and last QTD
def get_qtd_dates():
    now = datetime.now()
    qtd_start = now - relativedelta(months=(now.month - 1) % 3, day=1)
    qtd_end = now - timedelta(days=2)  # Exclude last 2 days for processing
    return qtd_start.strftime("%Y-%m-%dT00:00:00Z"), qtd_end.strftime("%Y-%m-%dT23:59:59Z")

# Get last year QTD dates based on current year QTD
def get_last_year_qtd_dates(qtd_start, qtd_end):
    last_year_start = (datetime.strptime(qtd_start, "%Y-%m-%dT%H:%M:%SZ") - relativedelta(years=1)).strftime("%Y-%m-%dT00:00:00Z")
    last_year_end = (datetime.strptime(qtd_end, "%Y-%m-%dT%H:%M:%SZ") - relativedelta(years=1)).strftime("%Y-%m-%dT23:59:59Z")
    return last_year_start, last_year_end

# Create directories for storing QTD reports
os.makedirs('qtdData/trafficData', exist_ok=True)

# Fetch report definitions for QTD using DAY periods
def get_report_definitions():
    qtd_start, qtd_end = get_qtd_dates()
    last_year_qtd_start, last_year_qtd_end = get_last_year_qtd_dates(qtd_start, qtd_end)

    print(f"QTD this year start: {qtd_start}, end: {qtd_end}")
    print(f"QTD last year start: {last_year_qtd_start}, end: {last_year_qtd_end}")

    reports = []

    # Split QTD this year into smaller chunks if needed
    qtd_chunks = split_date_range(datetime.strptime(qtd_start, "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime(qtd_end, "%Y-%m-%dT%H:%M:%SZ"))
    for idx, (start, end) in enumerate(qtd_chunks):
        reports.append({
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "DAY",
            },
            "report_name": f"qtd_this_year_part_{idx + 1}"
        })

    # Split LYQTD reports (matching current year's QTD date range)
    lyqtd_chunks = split_date_range(datetime.strptime(last_year_qtd_start, "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime(last_year_qtd_end, "%Y-%m-%dT%H:%M:%SZ"))
    for idx, (start, end) in enumerate(lyqtd_chunks):
        reports.append({
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "DAY",
            },
            "report_name": f"qtd_last_year_part_{idx + 1}"
        })

    return reports

def check_report_status(report_id):
    report_status_url = f"{endpoint}/reports/2021-06-30/reports/{report_id}"
    response = requests.get(report_status_url, headers=headers)
    response.raise_for_status()
    return response.json()

# Function to download reports
def download_report(report_id, report_type, fieldnames, report_name):
    retries = 3
    for _ in range(retries):
        try:
            status = check_report_status(report_id)
            while status["processingStatus"] not in ["DONE", "CANCELLED", "FATAL"]:
                print(f"Report status for {report_id}: {status['processingStatus']}")
                time.sleep(30)
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

                aggregate_csv_filename = f"qtdData/trafficData/{report_name}_aggregate_report.csv"
                with open(aggregate_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in traffic_aggregate_data:
                        row = {field: record.get(field, None) for field in fieldnames}
                        writer.writerow(row)

                asin_csv_filename = f"qtdData/trafficData/{report_name}_asin_report.csv"
                asin_fieldnames = list(traffic_by_asin_data[0].keys()) if traffic_by_asin_data else []
                with open(asin_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=asin_fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in traffic_by_asin_data:
                        writer.writerow(record)

                print(f"Report {report_type} downloaded and saved successfully.")
            else:
                print(f"Report processing for {report_id} failed with status: {status['processingStatus']}")
            break
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            print(f"Error occurred: {e}")
            time.sleep(10)
    else:
        print(f"Failed to download report {report_id} after {retries} retries.")

# Function to map ASINs to categories
def map_asins_to_categories(df, category_lookup_path=''):
    category_lookup = pd.read_csv(category_lookup_path)
    asin_to_category = dict(zip(category_lookup['ASIN'], category_lookup['Category']))
    df['Category'] = df['asin'].map(asin_to_category).fillna('Other')
    return df

# Compare QTD reports and calculate percentages by category
def compare_qtd_reports_by_category():
    this_year_df = pd.read_csv("qtdData/trafficData/qtd_this_year_asin_report.csv")
    last_year_df = pd.read_csv("qtdData/trafficData/qtd_last_year_asin_report.csv")

    this_year_df = map_asins_to_categories(this_year_df)
    last_year_df = map_asins_to_categories(last_year_df)

    this_year_grouped = this_year_df.groupby(['Category']).agg({'glanceViews': 'sum'}).reset_index()
    last_year_grouped = last_year_df.groupby(['Category']).agg({'glanceViews': 'sum'}).reset_index()

    merged_df = pd.merge(this_year_grouped, last_year_grouped, on='Category', suffixes=('_this_year', '_last_year'))

    merged_df['glanceViews_QoQ %'] = (
        (merged_df['glanceViews_this_year'] - merged_df['glanceViews_last_year']) /
        merged_df['glanceViews_last_year'] * 100
    ).fillna(0)

    merged_df.to_csv("qtdData/trafficData/qtd_comparison_by_category.csv", index=False)
    print("QTD over QTD percentages by category for traffic have been calculated and saved.")


# Create and download reports
def create_report(report_type, data_start_time, data_end_time, report_options):
    print(f"Creating report from {data_start_time} to {data_end_time}")
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
        report_id = report_response.json().get("reportId")
        print(f"Report created with ID: {report_id}")
        return report_id
    else:
        print(f"Error creating report: {report_response.json()}")
        return None

# Function to delete old folder contents
def delete_old_files(folder_path):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        os.makedirs(folder_path)
    print(f"Old files in {folder_path} have been deleted.")
    
# Main function to fetch and process reports, and compare them by category
def fetch_and_process_reports():
    delete_old_files('qtdData/trafficData')  # Clear out old data before fetching new reports
    reports_to_fetch = get_report_definitions()
    for report in reports_to_fetch:
        report_id = create_report(report["report_type"], report["start_time"], report["end_time"], report["report_options"])
        if report_id:
            download_report(report_id, report["report_type"], report["fieldnames"], report["report_name"])

    compare_qtd_reports_by_category()
    
def combine_traffic_reports():
    # Define the folder path
    traffic_folder = 'qtdData/trafficData'
    
    # Find all parts of this year's reports dynamically
    this_year_files = glob.glob(f"{traffic_folder}/qtd_this_year_part_*_asin_report.csv")
    
    # Combine this year's reports if files exist
    if this_year_files:
        this_year_dfs = [pd.read_csv(file) for file in sorted(this_year_files)]
        combined_this_year_df = pd.concat(this_year_dfs, ignore_index=True)
        combined_this_year_df.to_csv(f"{traffic_folder}/combined_qtd_this_year_traffic.csv", index=False)
        print("Combined QTD traffic data for this year saved.")
    else:
        print("No files for this year's traffic data found.")
    
    # Find all parts of last year's reports dynamically
    last_year_files = glob.glob(f"{traffic_folder}/qtd_last_year_part_*_asin_report.csv")
    
    # Combine last year's reports if files exist
    if last_year_files:
        last_year_dfs = [pd.read_csv(file) for file in sorted(last_year_files)]
        combined_last_year_df = pd.concat(last_year_dfs, ignore_index=True)
        combined_last_year_df.to_csv(f"{traffic_folder}/combined_qtd_last_year_traffic.csv", index=False)
        print("Combined QTD traffic data for last year saved.")
    else:
        print("No files for last year's traffic data found.")
        
if __name__ == "__main__":
    #fetch_and_process_reports()
    time.sleep(5)  # Wait for reports to be processed before combining
    combine_traffic_reports()
    

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
from pandas.tseries.offsets import MonthEnd
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
def split_date_range(start_date, end_date):
    max_days = 14
    chunks = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks

# Function to get YTD dates until the last full month
def get_ytd_dates():
    now = datetime.now()
    ytd_start = datetime(now.year, 1, 1)
    last_month_end = now.replace(day=1) - relativedelta(months=1) + MonthEnd(0)
    return ytd_start.strftime("%Y-%m-%dT00:00:00Z"), last_month_end.strftime("%Y-%m-%dT23:59:59Z")

# Function to get the remaining days in the current month (after last full month)
def get_remaining_days():
    now = datetime.now()
    last_month_end = now.replace(day=1) - relativedelta(months=1) + MonthEnd(0)
    remaining_start = last_month_end + timedelta(days=1)
    remaining_end = now - timedelta(days=2)
    return remaining_start, remaining_end

# Function to get LYTD dates until the last full month
def get_lytd_dates():
    now = datetime.now()
    last_year = now.year - 1
    lytd_start = datetime(last_year, 1, 1)
    last_year_last_month_end = datetime(last_year, now.month, 1) - relativedelta(months=1) + MonthEnd(0)
    return lytd_start.strftime("%Y-%m-%dT00:00:00Z"), last_year_last_month_end.strftime("%Y-%m-%dT23:59:59Z")

# Function to get LYTD remaining days (same period for last year)
def get_remaining_days_last_year():
    now = datetime.now()
    last_year = now.year - 1

    try:
        # Calculate the last month's end date for the previous year
        last_year_last_month_end = datetime(last_year, now.month, 1) - relativedelta(months=1) + MonthEnd(0)
        lytd_remaining_start = last_year_last_month_end + timedelta(days=1)

        # Ensure `now.day - 2` does not exceed the last day of the target month
        last_day_of_month = (datetime(last_year, now.month, 1) + MonthEnd(0)).day
        day_to_use = max(1, min(now.day - 2, last_day_of_month))

        # Create the lytd_remaining_end with the validated day
        lytd_remaining_end = datetime(last_year, now.month, day_to_use)

        return (
            lytd_remaining_start.strftime("%Y-%m-%dT00:00:00Z"),
            lytd_remaining_end.strftime("%Y-%m-%dT23:59:59Z")
        )
    except ValueError as e:
        print(f"Error in computing remaining days last year: {e}")
        raise

# Create directories for storing YTD reports
os.makedirs('ytdData/trafficData', exist_ok=True)

# Fetch report definitions for YTD and LYTD traffic data
def get_report_definitions():
    ytd_start, ytd_last_month_end = get_ytd_dates()
    remaining_days_start, remaining_days_end = get_remaining_days()
    lytd_start, lytd_last_month_end = get_lytd_dates()
    lytd_remaining_start, lytd_remaining_end = get_remaining_days_last_year()

    print(f"YTD start: {ytd_start}, end: {ytd_last_month_end}")
    print(f"Remaining days start: {remaining_days_start.strftime('%Y-%m-%dT00:00:00Z')}, end: {remaining_days_end.strftime('%Y-%m-%dT23:59:59Z')}")
    print(f"LYTD start: {lytd_start}, end: {lytd_last_month_end}")
    # No need to call strftime because lytd_remaining_start and lytd_remaining_end are already strings
    print(f"LYTD remaining start: {lytd_remaining_start}, end: {lytd_remaining_end}")

    reports = [
        # This year's full months
        {
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": ytd_start,
            "end_time": ytd_last_month_end,
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "MONTH"
            },
            "report_name": "ytd_this_year"
        },
        # Last year's full months
        {
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": lytd_start,
            "end_time": lytd_last_month_end,
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "MONTH"
            },
            "report_name": "lytd_last_year"
        }
    ]

    # For the remaining days this year, split into smaller reports if necessary
    if (remaining_days_end - remaining_days_start).days > 14:
        chunks = split_date_range(remaining_days_start, remaining_days_end)
        for idx, (start, end) in enumerate(chunks):
            reports.append({
                "report_type": "GET_VENDOR_TRAFFIC_REPORT",
                "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
                "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
                "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
                "report_options": {
                    "reportPeriod": "DAY"
                },
                "report_name": f"ytd_remaining_days_this_year_part_{idx + 1}"
            })
    else:
        reports.append({
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": remaining_days_start.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": remaining_days_end.strftime("%Y-%m-%dT23:59:59Z"),
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "DAY"
            },
            "report_name": "ytd_remaining_days_this_year"
        })

    # Same logic for last year’s remaining days
    if (datetime.strptime(lytd_remaining_end, "%Y-%m-%dT%H:%M:%SZ") - datetime.strptime(lytd_remaining_start, "%Y-%m-%dT%H:%M:%SZ")).days > 14:
        chunks = split_date_range(
            datetime.strptime(lytd_remaining_start, "%Y-%m-%dT%H:%M:%SZ"),
            datetime.strptime(lytd_remaining_end, "%Y-%m-%dT%H:%M:%SZ")
        )
        for idx, (start, end) in enumerate(chunks):
            reports.append({
                "report_type": "GET_VENDOR_TRAFFIC_REPORT",
                "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
                "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
                "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
                "report_options": {
                    "reportPeriod": "DAY"
                },
                "report_name": f"lytd_remaining_days_last_year_part_{idx + 1}"
            })
    else:
        reports.append({
            "report_type": "GET_VENDOR_TRAFFIC_REPORT",
            "start_time": lytd_remaining_start,
            "end_time": lytd_remaining_end,
            "fieldnames": ["startDate", "endDate", "asin", "glanceViews"],
            "report_options": {
                "reportPeriod": "DAY"
            },
            "report_name": "lytd_remaining_days_last_year"
        })

    return reports

# Create and download reports
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

# Download and process the report
def download_report(report_id, report_type, fieldnames, report_name):
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

                aggregate_csv_filename = f"ytdData/trafficData/{report_name}_aggregate_report.csv"
                with open(aggregate_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in traffic_aggregate_data:
                        row = {field: record.get(field, None) for field in fieldnames}
                        writer.writerow(row)

                asin_csv_filename = f"ytdData/trafficData/{report_name}_asin_report.csv"
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
            time.sleep(10)  # Wait for 10 seconds before retrying
    else:
        print(f"Failed to download report {report_id} after {retries} retries.")

# Merge and save ASIN report
def merge_asin_report(asin_csv_filename):
    asin_df = pd.read_csv(asin_csv_filename)
    merged_asin_df = asin_df.groupby('asin', as_index=False).agg({
        'glanceViews': 'sum'
    })
    merged_asin_df.to_csv(asin_csv_filename, index=False)

# Main function to fetch and process reports
def fetch_and_process_reports():
    folder_path = 'ytdData/trafficData'
    delete_old_files(folder_path)  # Delete old files before starting

    reports_to_fetch = get_report_definitions()
    for report in reports_to_fetch:
        report_id = create_report(report["report_type"], report["start_time"], report["end_time"], report["report_options"])
        if report_id:
            download_report(report_id, report["report_type"], report["fieldnames"], report["report_name"])

    # Combine YTD and LYTD reports

# Function to delete old folder contents
def delete_old_files(folder_path):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink():
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
    else:
        os.makedirs(folder_path)
    print(f"Old files in {folder_path} have been deleted.")
    
def combine_ytd_traffic_reports():
    # Define the folder path for YTD traffic reports
    traffic_folder = 'ytdData/trafficData'
    
    # Find all parts of this year's reports dynamically, including `ytd_this_year_asin_report.csv`
    this_year_files = glob.glob(f"{traffic_folder}/ytd_this_year_*_asin_report.csv") + [f"{traffic_folder}/ytd_this_year_asin_report.csv"]
    
    # Combine this year's reports if files exist
    if this_year_files:
        this_year_dfs = [pd.read_csv(file) for file in sorted(this_year_files) if os.path.exists(file)]
        if this_year_dfs:
            combined_this_year_df = pd.concat(this_year_dfs, ignore_index=True)
            combined_this_year_df.to_csv(f"{traffic_folder}/combined_ytd_this_year_traffic.csv", index=False)
            print("Combined YTD traffic data for this year saved.")
        else:
            print("No valid files found for this year's YTD traffic data.")
    else:
        print("No files for this year's YTD traffic data found.")
    
    # Find all parts of last year's reports dynamically, including `ytd_last_year_asin_report.csv`
    last_year_files = glob.glob(f"{traffic_folder}/lytd_*_asin_report.csv") + [f"{traffic_folder}/ytd_last_year_asin_report.csv"]
    
    # Combine last year's reports if files exist
    if last_year_files:
        last_year_dfs = [pd.read_csv(file) for file in sorted(last_year_files) if os.path.exists(file)]
        if last_year_dfs:
            combined_last_year_df = pd.concat(last_year_dfs, ignore_index=True)
            combined_last_year_df.to_csv(f"{traffic_folder}/combined_ytd_last_year_traffic.csv", index=False)
            print("Combined YTD traffic data for last year saved.")
        else:
            print("No valid files found for last year's YTD traffic data.")
    else:
        print("No files for last year's YTD traffic data found.")


if __name__ == "__main__":
    fetch_and_process_reports()
    combine_ytd_traffic_reports()
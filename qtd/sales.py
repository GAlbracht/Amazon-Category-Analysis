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
from pandas.tseries.offsets import QuarterEnd
from dateutil.relativedelta import relativedelta
import glob

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

# Function to split a large date range into smaller chunks (14-day max)
def split_date_range(start_date, end_date):
    max_days = 14
    chunks = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_days - 1), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    return chunks

# Function to get QTD dates for the current quarter until today
def get_qtd_dates():
    now = datetime.now()
    current_quarter_start = (now - relativedelta(months=(now.month - 1) % 3)).replace(day=1)
    qtd_end = now - timedelta(days=2)  # Excluding the last 2 days for processing time
    return current_quarter_start.strftime("%Y-%m-%dT00:00:00Z"), qtd_end.strftime("%Y-%m-%dT23:59:59Z")

# Function to get LYQTD dates (match the same period as current QTD last year)
def get_last_year_qtd_dates(qtd_start, qtd_end):
    last_year_start = (datetime.strptime(qtd_start, "%Y-%m-%dT%H:%M:%SZ") - relativedelta(years=1))
    last_year_end = (datetime.strptime(qtd_end, "%Y-%m-%dT%H:%M:%SZ") - relativedelta(years=1))
    return last_year_start.strftime("%Y-%m-%dT00:00:00Z"), last_year_end.strftime("%Y-%m-%dT23:59:59Z")

# Create directories for storing QTD reports
os.makedirs('qtdData', exist_ok=True)

# Fetch report definitions for QTD and LYQTD sales data
def get_report_definitions():
    qtd_start, qtd_end = get_qtd_dates()
    lyqtd_start, lyqtd_end = get_last_year_qtd_dates(qtd_start, qtd_end)

    print(f"QTD start: {qtd_start}, end: {qtd_end}")
    print(f"LYQTD start: {lyqtd_start}, end: {lyqtd_end}")

    reports = []

    # Split QTD reports into 14-day chunks
    qtd_chunks = split_date_range(datetime.strptime(qtd_start, "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime(qtd_end, "%Y-%m-%dT%H:%M:%SZ"))
    for idx, (start, end) in enumerate(qtd_chunks):
        reports.append({
            "report_type": "GET_VENDOR_SALES_REPORT",
            "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
            "fieldnames": ["startDate", "endDate", "asin", "shippedCogs", "shippedRevenue", "shippedUnits"],
            "report_options": {
                "reportPeriod": "DAY",
                "distributorView": "SOURCING",
                "sellingProgram": "RETAIL"
            },
            "report_name": f"qtd_this_year_part_{idx + 1}"
        })

    # Split LYQTD reports into 14-day chunks (matching this year's QTD)
    lyqtd_chunks = split_date_range(datetime.strptime(lyqtd_start, "%Y-%m-%dT%H:%M:%SZ"), datetime.strptime(lyqtd_end, "%Y-%m-%dT%H:%M:%SZ"))
    for idx, (start, end) in enumerate(lyqtd_chunks):
        reports.append({
            "report_type": "GET_VENDOR_SALES_REPORT",
            "start_time": start.strftime("%Y-%m-%dT00:00:00Z"),
            "end_time": end.strftime("%Y-%m-%dT23:59:59Z"),
            "fieldnames": ["startDate", "endDate", "asin", "shippedCogs", "shippedRevenue", "shippedUnits"],
            "report_options": {
                "reportPeriod": "DAY",
                "distributorView": "SOURCING",
                "sellingProgram": "RETAIL"
            },
            "report_name": f"lyqtd_last_year_part_{idx + 1}"
        })

    return reports

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
                
                sales_aggregate_data = report_json.get("salesAggregate", [])
                sales_by_asin_data = report_json.get("salesByAsin", [])

                aggregate_csv_filename = f"qtdData/{report_name}_aggregate_report.csv"
                with open(aggregate_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in sales_aggregate_data:
                        record['shippedCogs'] = record['shippedCogs']['amount'] if isinstance(record['shippedCogs'], dict) else 0
                        record['shippedRevenue'] = record['shippedRevenue']['amount'] if isinstance(record['shippedRevenue'], dict) else 0
                        row = {field: record.get(field, None) for field in fieldnames}
                        writer.writerow(row)

                asin_csv_filename = f"qtdData/{report_name}_asin_report.csv"
                asin_fieldnames = list(sales_by_asin_data[0].keys()) if sales_by_asin_data else []
                with open(asin_csv_filename, "a", newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=asin_fieldnames)
                    if csvfile.tell() == 0:
                        writer.writeheader()
                    for record in sales_by_asin_data:
                        record['shippedCogs'] = record['shippedCogs']['amount'] if isinstance(record['shippedCogs'], dict) else 0
                        record['shippedRevenue'] = record['shippedRevenue']['amount'] if isinstance(record['shippedRevenue'], dict) else 0
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

# Merge and save ASIN report
def merge_asin_report(asin_csv_filename):
    if not os.path.exists(asin_csv_filename):
        print(f"Error: {asin_csv_filename} does not exist.")
        return
    
    asin_df = pd.read_csv(asin_csv_filename)
    merged_asin_df = asin_df.groupby('asin', as_index=False).agg({
        'shippedCogs': 'sum',
        'shippedRevenue': 'sum',
        'shippedUnits': 'sum',
    })
    merged_asin_df.to_csv(asin_csv_filename, index=False)

# Main function to fetch and process reports
def fetch_and_process_reports():
    folder_path = 'qtdData'
    delete_old_files(folder_path)  # Delete old files before starting

    reports_to_fetch = get_report_definitions()
    for report in reports_to_fetch:
        report_id = create_report(report["report_type"], report["start_time"], report["end_time"], report["report_options"])
        if report_id:
            download_report(report_id, report["report_type"], report["fieldnames"], report["report_name"])

    # Combine QTD and LYQTD reports
    combine_qtd_lyqtd_reports()

# Function to combine QTD and LYQTD reports
def combine_qtd_lyqtd_reports():
    try:
        qtd_df = pd.read_csv("qtdData/qtd_this_year_asin_report.csv")
    except FileNotFoundError:
        print("qtd_this_year_asin_report.csv not found, skipping merge.")
        return

    try:
        lyqtd_df = pd.read_csv("qtdData/lyqtd_last_year_asin_report.csv")
    except FileNotFoundError:
        print("lyqtd_last_year_asin_report.csv not found, skipping merge.")
        return
    

    # Combine QTD and remaining days for this year
    combined_qtd = pd.concat([qtd_df])

    # Combine LYQTD and remaining days for last year
    combined_lyqtd = pd.concat([lyqtd_df])

    # Save combined reports
    combined_qtd.to_csv("qtdData/combined_qtd_this_year.csv", index=False)
    combined_lyqtd.to_csv("qtdData/combined_lyqtd_last_year.csv", index=False)

    print("Combined QTD and LYQTD reports saved.")


def combine_ytd_lytd_reports():
    ytd_df = pd.read_csv("qtdData/ytd_this_year_asin_report.csv")
    lytd_df = pd.read_csv("qtdData/lytd_last_year_full_months_asin_report.csv")

    # Combine YTD and remaining days for this year
    combined_ytd = pd.concat([ytd_df])

    # Combine LYTD and remaining days for last year
    combined_lytd = pd.concat([lytd_df])

    # Save combined reports
    combined_ytd.to_csv("qtdData/combined_ytd_this_year.csv", index=False)
    combined_lytd.to_csv("qtdData/combined_lytd_last_year.csv", index=False)

    print("Combined YTD and LYTD reports saved.")


def combine_sales_reports():
    # Define the folder path
    sales_folder = 'qtdData'
    
    # Find all parts of this year's reports dynamically
    this_year_files = glob.glob(f"{sales_folder}/qtd_this_year_part_*_asin_report.csv")
    
    # Combine this year's reports if files exist
    if this_year_files:
        this_year_dfs = [pd.read_csv(file) for file in sorted(this_year_files)]
        combined_this_year_df = pd.concat(this_year_dfs, ignore_index=True)
        combined_this_year_df.to_csv(f"{sales_folder}/combined_qtd_this_year_sales.csv", index=False)
        print("Combined QTD sales data for this year saved.")
    else:
        print("No files for this year's sales data found.")
    
    # Find all parts of last year's reports dynamically
    last_year_files = glob.glob(f"{sales_folder}/lyqtd_last_year_part_*_asin_report.csv")
    
    # Combine last year's reports if files exist
    if last_year_files:
        last_year_dfs = [pd.read_csv(file) for file in sorted(last_year_files)]
        combined_last_year_df = pd.concat(last_year_dfs, ignore_index=True)
        combined_last_year_df.to_csv(f"{sales_folder}/combined_qtd_last_year_sales.csv", index=False)
        print("Combined QTD sales data for last year saved.")
    else:
        print("No files for last year's sales data found.")
        
if __name__ == "__main__":
    fetch_and_process_reports()
    combine_sales_reports()

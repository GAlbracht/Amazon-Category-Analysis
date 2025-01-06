import pandas as pd
import os
import shutil

# Load the category mapping file
category_mapping_file = ''
category_df = pd.read_csv(category_mapping_file)

# Ensure the mergedReports directory exists
merged_reports_folder = 'mergedReports'
if not os.path.exists(merged_reports_folder):
    os.makedirs(merged_reports_folder)

def delete_merged_reports_folder_contents(folder_path):
    """
    Deletes all contents inside the specified folder.
    """
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
    print(f"Old files in {folder_path} have been deleted.")

def calculate_yoy_kpis(this_year_data, last_year_data, category_mapping_df, key_columns):
    """
    Function to calculate year-over-year KPIs for both sales and traffic.
    """
    # Ensure the ASIN column is consistent across all DataFrames
    if 'ASIN' in category_mapping_df.columns:
        category_mapping_df.rename(columns={'ASIN': 'asin'}, inplace=True)

    # Merge the category mapping with both this year and last year's data
    this_year_data = pd.merge(this_year_data, category_mapping_df, on="asin", how="left")
    last_year_data = pd.merge(last_year_data, category_mapping_df, on="asin", how="left")

    # Fill missing categories with 'Unknown'
    this_year_data['Category'] = this_year_data['Category'].fillna('Unknown')
    last_year_data['Category'] = last_year_data['Category'].fillna('Unknown')

    # Group by Category and sum the KPIs
    this_year_grouped = this_year_data.groupby('Category').agg({col: 'sum' for col in key_columns}).reset_index()
    last_year_grouped = last_year_data.groupby('Category').agg({col: 'sum' for col in key_columns}).reset_index()

    # Merge the grouped data on Category
    merged_df = pd.merge(this_year_grouped, last_year_grouped, on='Category', suffixes=('_this_year', '_last_year'))

    # Calculate YoY percentage changes for each KPI
    for col in key_columns:
        merged_df[f'YoY % {col}'] = ((merged_df[f'{col}_this_year'] - merged_df[f'{col}_last_year']) / 
                                     merged_df[f'{col}_last_year']) * 100
        merged_df[f'YoY % {col}'] = merged_df[f'YoY % {col}'].fillna(0)  # Handle NaN from division by zero
    
    return merged_df

def process_yoy_data(this_year_file, last_year_file, key_columns):
    """
    Process and calculate YoY KPIs for both sales and traffic data.
    """
    if not os.path.exists(this_year_file) or not os.path.exists(last_year_file):
        print(f"Files not found: {this_year_file}, {last_year_file}")
        return None
    
    # Load this year's and last year's data
    this_year_data = pd.read_csv(this_year_file)
    last_year_data = pd.read_csv(last_year_file)

    # Calculate YoY KPIs using the category mapping
    result_df = calculate_yoy_kpis(this_year_data, last_year_data, category_df, key_columns)

    return result_df

def merge_sales_and_traffic(sales_yoy, traffic_yoy):
    """
    Merge sales and traffic data by Category, and calculate combined YoY KPIs.
    """
    # Merge sales and traffic on Category
    combined_df = pd.merge(sales_yoy, traffic_yoy, on='Category', suffixes=('_sales', '_traffic'))

    # Select necessary columns and calculate combined YoY KPIs
    columns_to_select = [
        'Category', 
        'shippedCogs_this_year', 'shippedCogs_last_year', 'YoY % shippedCogs',
        'shippedRevenue_this_year', 'shippedRevenue_last_year', 'YoY % shippedRevenue',
        'shippedUnits_this_year', 'shippedUnits_last_year', 'YoY % shippedUnits',
        'glanceViews_this_year', 'glanceViews_last_year', 'YoY % glanceViews'
    ]
    combined_df = combined_df[columns_to_select]

    return combined_df

# Define key columns for sales and traffic
sales_key_columns = ['shippedCogs', 'shippedRevenue', 'shippedUnits']
traffic_key_columns = ['glanceViews']

# Delete old files in the mergedReports folder
delete_merged_reports_folder_contents(merged_reports_folder)

# Process YoY data for YTD sales
ytd_sales_yoy = process_yoy_data('ytdData/combined_ytd_this_year.csv', 
                                 'ytdData/combined_ytd_last_year.csv', 
                                 sales_key_columns)

# Process YoY data for YTD traffic
ytd_traffic_yoy = process_yoy_data('ytdData/trafficData/combined_ytd_this_year_traffic.csv', 
                                   'ytdData/trafficData/combined_ytd_last_year_traffic.csv', 
                                   traffic_key_columns)

# Merge YTD sales and traffic data
if ytd_sales_yoy is not None and ytd_traffic_yoy is not None:
    combined_ytd_df = merge_sales_and_traffic(ytd_sales_yoy, ytd_traffic_yoy)
    combined_ytd_file = os.path.join(merged_reports_folder, 'combined_yoy_ytd_sales_traffic.csv')
    combined_ytd_df.to_csv(combined_ytd_file, index=False)
    print(f"Combined YTD sales and traffic YoY KPI data saved to {combined_ytd_file}")

# Process YoY data for QTD sales
qtd_sales_yoy = process_yoy_data('qtdData/combined_qtd_this_year_sales.csv', 
                                 'qtdData/combined_qtd_last_year_sales.csv', 
                                 sales_key_columns)

# Process YoY data for QTD traffic
qtd_traffic_yoy = process_yoy_data('qtdData/trafficData/combined_qtd_this_year_traffic.csv', 
                                   'qtdData/trafficData/combined_qtd_last_year_traffic.csv', 
                                   traffic_key_columns)

# Merge QTD sales and traffic data
if qtd_sales_yoy is not None and qtd_traffic_yoy is not None:
    combined_qtd_df = merge_sales_and_traffic(qtd_sales_yoy, qtd_traffic_yoy)
    combined_qtd_file = os.path.join(merged_reports_folder, 'combined_yoy_qtd_sales_traffic.csv')
    combined_qtd_df.to_csv(combined_qtd_file, index=False)
    print(f"Combined QTD sales and traffic YoY KPI data saved to {combined_qtd_file}")

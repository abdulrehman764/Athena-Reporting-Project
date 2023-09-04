import boto3
import os
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
# from fpdf import FPDF

# AWS credentials
aws_access_key_id = 'ASIAYMSB3P7B4CBBIQXM'
aws_secret_access_key = 'VpUD+DpQNqYxrNaftJ1j6gT/7/rHscjKVWthAS8t'
aws_session_token = 'IQoJb3JpZ2luX2VjEGsaCXVzLWVhc3QtMSJHMEUCIFrTR1Qqz16vO3+qstRLuir1AUZ58tHA8aIT90h6P9q7AiEAlfoD+VqFCvuKZeXv1E2ZQFM8FFWLQIB6TwFHAF6O6yAqgwMIQxAAGgw1NzY3Mzc0NzY1NDciDIQkznLhotTNYFArqCrgAjXGdFMrUJmilXwZosd4fGUHdU3YrOiImL0BPlWEW1l5ppJ2acgL/nM5K1ZV0od8yo6fDiP254GoY5OUnkFCElnU5Fm4q4DUCj7dX25KLD2vi7wmq6CMWEfn+Imir9XL+tA6Ly2STcHorYiaMFQd5BB0mijiv13uO0uKz9o9Qz0wxlLp5y/P99U5VOvw2DyRwZpNhi2LE76/im1Cmzycir46HlTQL6+48GK+H+sE/yb7Q5I/F8ovtqakV+BpYFnq6wrqDHx/Km6rNe+mjGeV3yFqkp/wmsfiyMx0hrO6SVzrWPQpCNQRO9BhCzfTdjCNC8t/vQkH5AOnvAfpkGNn224g1QCOS+Z9IFl+RgakqYgzfxzwj/Pe+CuM1zWvSRmdJnY5xEqoX4LosBe2tQ765ci5hb9mPmQGv9oKugTCSwGYl+YWwUGjSYlEPis3VzCZ5o2PUmXmeChGMfkbrgoxsGUwgNnWpwY6pgEJRNIfVH1NRMfEdHC1y4NGlgU20Q5Zk1zTJfmbrewZ9YnkQNygU2XZsEqe/zWVo6yRMNB5CxLyQK/4mdFSAoMgWrYQXVWBlID+QLapXJsqGi3HgPRaAqFRPZQpbTvWQ7ByYndQqZwuTnsDrzBmdgMP/+iqg2gcObKeYnsvc/BMJXMNb+wBA/8mV3zhrbLEVm8euj0/KivsBQO8rncDFK9j1EfY3HtT'
s3_bucket = 'athena-report-123'
athena_database = 'abdul-csv-parquet-db'
athena_table = 'gamechangeoutput'




# Initialize AWS Athena and S3 clients
athena_client = boto3.client('athena', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key,
                             aws_session_token=aws_session_token, region_name='us-east-1')

s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key,
                         aws_session_token=aws_session_token, region_name='us-east-1')

# Define the output location
output_location = f's3://{s3_bucket}/athena-query-results/'

def run_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': athena_database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for the query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = query_status['QueryExecution']['Status']['State']

        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    return query_execution_id, state

def plot_total_daily_yield_by_state(query_execution_id):
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract and print the column names
    column_names = [col['Name'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print('\t'.join(column_names))

    # Extract and print the rows of data
    data_rows = []
    for row in query_results['ResultSet']['Rows'][1:]:
        values = [data['VarCharValue'] for data in row['Data']]
        data_rows.append(values)
        print('\t'.join(values))

    # Create a DataFrame from the data
    df = pd.DataFrame(data_rows, columns=column_names)

    # Convert 'total_daily_yield' column to numeric
    df['total_daily_yield'] = pd.to_numeric(df['total_daily_yield'])

    # Plot the data using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.bar(df['state'], df['total_daily_yield'])
    plt.xlabel('State')
    plt.ylabel('Total Daily Yield')
    plt.title('Total Daily Yield by State')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image
    plot_image_path = 'athena_query_plot1.png'
    plt.savefig(plot_image_path)
    return plot_image_path

def plot_avg_temperature_by_state(query_execution_id):
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract and print the column names
    column_names = [col['Name'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print('\t'.join(column_names))

    # Extract and print the rows of data
    data_rows = []
    for row in query_results['ResultSet']['Rows'][1:]:
        values = [data['VarCharValue'] for data in row['Data']]
        data_rows.append(values)
        print('\t'.join(values))

    # Create a DataFrame from the data
    df = pd.DataFrame(data_rows, columns=column_names)

    # Convert 'avg_temperature' column to numeric
    df['avg_temperature'] = pd.to_numeric(df['avg_temperature'])

    # Plot the data using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.bar(df['state'], df['avg_temperature'], color='skyblue')
    plt.xlabel('State')
    plt.ylabel('Average Temperature')
    plt.title('Average Temperature by State')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image
    plot_image_path = 'athena_query_plot2.png'
    plt.savefig(plot_image_path)
    return plot_image_path

def plot_avg_daily_yield_by_city(query_execution_id):
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract and print the column names
    column_names = [col['Name'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print('\t'.join(column_names))

    # Extract and print the rows of data
    data_rows = []
    for row in query_results['ResultSet']['Rows'][1:]:
        values = [data['VarCharValue'] for data in row['Data']]
        data_rows.append(values)
        print('\t'.join(values))

    # Create a DataFrame from the data
    df = pd.DataFrame(data_rows, columns=column_names)

    # Convert 'avg_daily_yield' column to numeric
    df['avg_daily_yield'] = pd.to_numeric(df['avg_daily_yield'])

    # Sort the DataFrame by 'avg_daily_yield' in descending order for better visualization
    df = df.sort_values(by='avg_daily_yield', ascending=False)

    # Plot the data using Matplotlib
    plt.figure(figsize=(12, 6))
    plt.bar(df['city'], df['avg_daily_yield'], color='skyblue')
    plt.xlabel('City')
    plt.ylabel('Average Daily Yield')
    plt.title('Average Daily Yield by City')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save the plot as an image
    plot_image_path = 'athena_query_plot3.png'
    plt.savefig(plot_image_path)
    return plot_image_path

def plot_correlation_between_temperature_and_daily_yield(query_execution_id):
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract and print the column names
    column_names = [col['Name'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print('\t'.join(column_names))

    # Extract and print the rows of data
    data_rows = []
    for row in query_results['ResultSet']['Rows'][1:]:
        values = [data['VarCharValue'] for data in row['Data']]
        data_rows.append(values)
        print('\t'.join(values))

    # Create a DataFrame from the data
    df = pd.DataFrame(data_rows, columns=column_names)

    # Convert 'temperature' and 'avg_daily_yield' columns to numeric
    df['temperature'] = pd.to_numeric(df['temperature'])
    df['avg_daily_yield'] = pd.to_numeric(df['avg_daily_yield'])

    # Sort the DataFrame by 'temperature' in ascending order for better visualization
    df = df.sort_values(by='temperature')

    # Plot the data using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.plot(df['temperature'], df['avg_daily_yield'], marker='o', color='skyblue')
    plt.xlabel('Temperature')
    plt.ylabel('Average Daily Yield')
    plt.title('Correlation between Temperature and Daily Yield')
    plt.grid(True)
    plt.tight_layout()

    # Save the plot as an image
    plot_image_path = 'athena_query_plot4.png'
    plt.savefig(plot_image_path)
    return plot_image_path

def plot_sunlight_intensity_vs_ac_power(query_execution_id):
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Extract and print the column names
    column_names = [col['Name'] for col in query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print('\t'.join(column_names))

    # Extract and print the rows of data
    data_rows = []
    for row in query_results['ResultSet']['Rows'][1:]:
        values = [data['VarCharValue'] for data in row['Data']]
        data_rows.append(values)
        print('\t'.join(values))

    # Create a DataFrame from the data
    df = pd.DataFrame(data_rows, columns=column_names)

    # Convert 'sunlightintensity' and 'avg_acpower' columns to numeric
    df['sunlightintensity'] = pd.to_numeric(df['sunlightintensity'])
    df['avg_acpower'] = pd.to_numeric(df['avg_acpower'])

    # Sort the DataFrame by 'sunlightintensity' in ascending order for better visualization
    df = df.sort_values(by='sunlightintensity')

    # Plot the data using Matplotlib
    plt.figure(figsize=(10, 6))
    plt.plot(df['sunlightintensity'], df['avg_acpower'], marker='o', color='skyblue')
    plt.xlabel('Sunlight Intensity')
    plt.ylabel('Average AC Power')
    plt.title('Sunlight Intensity vs AC Power')
    plt.grid(True)
    plt.tight_layout()

    # Save the plot as an image
    plot_image_path = 'athena_query_plot5.png'
    plt.savefig(plot_image_path)
    return plot_image_path


# Create a PDF report containing all the plots
def create_pdf_report(plot_image_paths, pdf_output_path):
    c = canvas.Canvas(pdf_output_path, pagesize=landscape(letter))
    c.setFont("Helvetica", 14)

    for image_path in plot_image_paths:
        c.drawImage(image_path, 50, 50, width=500, height=350)
        c.showPage()

    c.save()

    return pdf_output_path




# Query 1
query1 = """SELECT state, SUM(CAST(dailyyield AS INT)) AS total_daily_yield
            FROM gamechangeoutput
            GROUP BY state
         """
query_execution_id, query_state = run_athena_query(query1)

if query_state == 'SUCCEEDED':
    plot_image_path1 = plot_total_daily_yield_by_state(query_execution_id)
    s3_key = 'athena-query-plot1.png'
    s3_client.upload_file(plot_image_path1, s3_bucket, s3_key)
    # os.remove(plot_image_path1)
else:
    print(f"Query 1 failed with state: {query_state}")

# Query 2
query2 = """SELECT state, AVG(CAST(temperature AS INT)) AS avg_temperature
            FROM gamechangeoutput
            GROUP BY state
         """
query_execution_id, query_state = run_athena_query(query2)

if query_state == 'SUCCEEDED':
    plot_image_path2 = plot_avg_temperature_by_state(query_execution_id)
    s3_key = 'athena-query-plot2.png'
    s3_client.upload_file(plot_image_path2, s3_bucket, s3_key)
    # os.remove(plot_image_path2)
else:
    print(f"Query 2 failed with state: {query_state}")

# Query 3
query3 = """SELECT city, AVG(CAST(dailyyield AS INT)) AS avg_daily_yield
            FROM gamechangeoutput
            GROUP BY city
            ORDER BY avg_daily_yield DESC
         """
query_execution_id, query_state = run_athena_query(query3)

if query_state == 'SUCCEEDED':
    plot_image_path3 = plot_avg_daily_yield_by_city(query_execution_id)
    s3_key = 'athena-query-plot3.png'
    s3_client.upload_file(plot_image_path3, s3_bucket, s3_key)
    # os.remove(plot_image_path3)
else:
    print(f"Query 3 failed with state: {query_state}")

# Query 4
query4 = """SELECT CAST(temperature AS INT) AS temperature, AVG(CAST(dailyyield AS INT)) AS avg_daily_yield
            FROM gamechangeoutput
            GROUP BY CAST(temperature AS INT)
            ORDER BY CAST(temperature AS INT)
         """
query_execution_id, query_state = run_athena_query(query4)

if query_state == 'SUCCEEDED':
    plot_image_path4 = plot_correlation_between_temperature_and_daily_yield(query_execution_id)
    s3_key = 'athena-query-plot4.png'
    s3_client.upload_file(plot_image_path4, s3_bucket, s3_key)
    # os.remove(plot_image_path4)
else:
    print(f"Query 4 failed with state: {query_state}")

# Query 5
query5 = """SELECT sunlightintensity, AVG(CAST(acpower AS INT)) AS avg_acpower
            FROM gamechangeoutput
            GROUP BY sunlightintensity
            ORDER BY sunlightintensity
         """
query_execution_id, query_state = run_athena_query(query5)

if query_state == 'SUCCEEDED':
    plot_image_path5 = plot_sunlight_intensity_vs_ac_power(query_execution_id)
    s3_key = 'athena-query-plot5.png'
    s3_client.upload_file(plot_image_path5, s3_bucket, s3_key)
    # os.remove(plot_image_path5)
else:
    print(f"Query 5 failed with state: {query_state}")



# Create a PDF report with all the plots
plot_image_paths = [plot_image_path1, plot_image_path2, plot_image_path3, plot_image_path4, plot_image_path5]
pdf_output_path = 'athena_query_report.pdf'
pdf_output_path = create_pdf_report(plot_image_paths, pdf_output_path)

# Upload the PDF report to S3
s3_key = 'athena-query-report.pdf'
s3_client.upload_file(pdf_output_path, s3_bucket, s3_key)
# os.remove(pdf_output_path)

print(f'PDF report uploaded to S3://{s3_bucket}/{s3_key}')
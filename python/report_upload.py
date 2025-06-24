import os
import csv
import time
import re
import click
import timeplus_connect

db_name = 'nexmark'
stats_stream_name = 'nexmark_benchmark_stats_report'
result_stream_name  = 'nexmark_benchmark_result_report'

timeplus_host = os.getenv('TIMEPLUS_HOST', 'localhost')
timeplus_port = os.getenv('TIMEPLUS_PORT', '3218')
timeplus_user = os.getenv('TIMEPLUS_USER', 'proton')
timeplus_password = os.getenv('TIMEPLUS_PASSWORD', 'timeplus@t+')
client = timeplus_connect.get_client(host=timeplus_host, port=int(timeplus_port), user=timeplus_user, password=timeplus_password)

def extract_time_from_filename(filename):
    # Pattern: report_ followed by any characters, ending with .csv
    pattern = r'^report_(.+)\.csv$'
    match = re.match(pattern, filename)
    
    if match:
        return match.group(1)  # Returns the time part
    else:
        return None
    
def get_last_report_time():
    #list file name in current directory
    files = os.listdir('.')
    report_times = []
    for file in files:
        if file.startswith('report_') and file.endswith('.csv'):
            report_time = extract_time_from_filename(file)
            if report_time:
                report_times.append(report_time)
                
    report_times.sort()
    last_report_time = report_times[-1]
    return last_report_time

@click.command()
@click.option('--report_time', help='the time of the report to be uploaded')
@click.option('--init_stream', default=True, help='whether to initialize the stream, default to false')
def main(report_time, init_stream):
    report_time = report_time or get_last_report_time()
    print(f"using report time {report_time}")
    report_file_name = f'stats_report_{report_time}.json'
    result_file_name = f'report_{report_time}.csv'

    if init_stream:
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        except Exception as e:
            print(f'faile to create db {e}')
            exit(1)
            
        try:
            client.command(f"CREATE STREAM IF NOT EXISTS {db_name}.{stats_stream_name} (raw string, report_time string)")
        except Exception as e:
            print(f'faile to create stats stream {e}')
            exit(1)

        try:  # create a new result stream
            client.command(f"""CREATE STREAM IF NOT EXISTS {db_name}.{result_stream_name} (
                               report_time string,
                               case string,
                               platform string,
                               execution_time float64,
                               output_size int,
                               error string
                            )
                           """)
        except Exception as e:
            print(f'faile to create result stream {e}')
            exit(1)
    
    # wait for the stream to be created        
    time.sleep(3)
        
    with open(report_file_name, 'r') as file:
        batch = []
        for line in file:
            # Load each line as a JSON object
            row = line.strip()
            batch.append([row, report_time])
            if len(batch) == 100:
                try:
                    client.insert(stats_stream_name, batch, column_names=['raw', 'report_time'], database=db_name)
                    time.sleep(0.5)
                except Exception as e:
                    print(f'failed to ingest {e}')
                finally:
                    batch = []
                    
            try:
                client.insert(stats_stream_name, batch, column_names=['raw', 'report_time'], database=db_name)
                time.sleep(0.5)
            except Exception as e:
                print(f'failed to ingest {e}')
                
        print("stats data uploaded")
                    
    with open(result_file_name, newline='') as csvfile:
        result_reader = csv.reader(csvfile)
        next(result_reader)
        for row in result_reader:
            #print(row)
            data = [report_time] + row
            try:
                client.insert(result_stream_name, [data], column_names=["report_time", "case","platform","execution_time","output_size","error"], database=db_name)
                time.sleep(0.5)
            except Exception as e:
                print(f'failed to ingest {e}')
                
        print("result data uploaded")

if __name__ == '__main__':
    main()
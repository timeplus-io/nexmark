import os
import csv
import click
from timeplus import Stream, Environment

api_key = os.environ.get("TIMEPLUS_API_KEY")
api_address = os.environ.get("TIMEPLUS_ADDRESS")
env = Environment().address(api_address).apikey(api_key)

@click.command()
@click.option('--report_time', help='the time of the report to be uploaded')
def main(report_time):
    report_file_name = f'stats_report{report_time}.json'
    result_file_name = f'report{report_time}.csv'
    stats_stream_name = 'benchmark_stats_report'
    result_stream_name  = 'benchmark_result_report'

    stats_stream = None
    result_stream = None

    try:
        # create a new stats stream
        stats_stream = (
            Stream(env=env)
            .name(stats_stream_name)
            .column("raw", "string")
            .column("report_time", "string")
            .create()
        )
    except Exception as e:
        stats_stream = (Stream(env=env)
            .name(stats_stream_name)
            .column("raw", "string")
            .column("report_time", "string")
        )

    with open(report_file_name, 'r') as file:
        for line in file:
            # Load each line as a JSON object
            row = line.strip()
            stats_stream.ingest(["raw","report_time"], [[row, report_time]])

    try:
        # create a new result stream
        result_stream = (
            Stream(env=env)
            .name(result_stream_name)
            .column("report_time", "string")
            .column("case", "string")
            .column("platform", "string")
            .column("time", "float64")
            .column("size", "int")
            .create()
        )
    except Exception as e:
        result_stream = (
            Stream(env=env)
            .name(result_stream_name)
            .column("report_time", "string")
            .column("case", "string")
            .column("platform", "string")
            .column("time", "float64")
            .column("size", "int")
        )

    with open(result_file_name, newline='') as csvfile:
        result_reader = csv.reader(csvfile)
        next(result_reader)
        for row in result_reader:
            print(row)
            result_stream.ingest(["case","platform","time","size","report_time"], [row + [report_time]])

    

if __name__ == '__main__':
    main()
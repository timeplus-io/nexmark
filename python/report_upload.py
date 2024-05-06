import os
import json
import click
from timeplus import Stream, Environment

api_key = os.environ.get("TIMEPLUS_API_KEY")
api_address = os.environ.get("TIMEPLUS_ADDRESS")
env = Environment().address(api_address).apikey(api_key)

@click.command()
@click.option('--report_time', help='the time of the report to be uploaded')
def main(report_time):
    report_file_name = f'stats_report{report_time}.json'
    stream_name = f'benchmark_stats_report_{report_time}'

    try:
        # create a new stream
        stream = (
            Stream(env=env)
            .name(stream_name)
            .column("raw", "string")
            .create()
        )

        with open(report_file_name, 'r') as file:
            for line in file:
                # Load each line as a JSON object
                row = line.strip()
                stream.ingest(["raw"], [[row]])
    except Exception as e:
        print(e)


    

if __name__ == '__main__':
    main()
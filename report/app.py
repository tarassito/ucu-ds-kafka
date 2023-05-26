import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO)


def create_report():
    files = Path('logs/').glob('*.csv')
    dfs = [pd.read_csv(f, names=['sent_timestamp', 'processed_timestamp', 'value',
                                 'size']) for f in files]

    df = pd.concat(dfs, ignore_index=True)

    run_time = df['processed_timestamp'].max() - df['processed_timestamp'].min()
    bytes_sent = sum(df['size'])
    throughput_bps = bytes_sent / run_time
    throughput_mbps = (throughput_bps * 8) / 1000000

    max_latency = (df['processed_timestamp'] - df['sent_timestamp']).max()

    logging.info(f"<<<ANALYTICS>>>\nBytes sent: {bytes_sent} b\nRun time: {run_time} s\nThroughput: {throughput_mbps} Mbps\
          \nMax msg processing latency: {max_latency}")


if __name__ == '__main__':
    create_report()

import asyncio
from aiokafka import AIOKafkaProducer
import aiofiles
import json
from pathlib import Path
import argparse

# TPC-H Table Definitions
table_columns = {
    "region": [
        "r_regionkey",
        "r_name",
        "r_comment"
    ],
    "nation": [
        "n_nationkey",
        "n_name",
        "n_regionkey",
        "n_comment"
    ],
    "part": [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment"
    ],
    "supplier": [
        "s_suppkey",
        "s_name",
        "s_address",
        "s_nationkey",
        "s_phone",
        "s_acctbal",
        "s_comment"
    ],
    "partsupp": [
        "ps_partkey",
        "ps_suppkey",
        "ps_availqty",
        "ps_supplycost",
        "ps_comment"
    ],
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment"
    ],
    "orders": [
        "o_orderkey",
        "o_custkey",
        "o_orderstatus",
        "o_totalprice",
        "o_orderdate",
        "o_orderpriority",
        "o_clerk",
        "o_shippriority",
        "o_comment"
    ],
    "lineitem": [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
        "l_comment"
    ]
}

class TPCHAsyncKafkaProducer:
    def __init__(self, data_path, kafka_bootstrap_servers='localhost:9092'):
        self.data_path = data_path
        self.bootstrap_servers = kafka_bootstrap_servers
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='snappy'
        )
        await self.producer.start()
    
    async def stream_table(self, table_name, rate_limit=10000):
        """
        Automatically stream a table by name
        Finds: data/{table_name}.tbl and uses table_columns for schema
        """
        if table_name not in table_columns:
            raise ValueError(f"Unknown table: {table_name}")

        column_names = table_columns[table_name]
        print(self.data_path) # /home/greenlakehouse/demo-video/dataset/TPC/ref_data/1/supplier
        print(table_name) # supplier
        data_files = sorted(Path(self.data_path).glob(f"{table_name}.tbl.*"))

        if not data_files:
            raise FileNotFoundError(f"No data files found for {table_name} in {self.data_path}")

        print(f"Found {len(data_files)} files for table {table_name}:")

        print(f"Streaming table: {table_name}")
        print(f"  Schema: {column_names}")
        
        # Stream to Kafka
        topic_name = table_name
        idx = 0

        for data_file in data_files:
            #print(f"  Processing file: {data_file}")

            async with aiofiles.open(data_file, mode='r') as f:
                async for line in f:
                    values = line.strip().split('|')
                    # Handle trailing pipe if present
                    if len(values) > len(column_names):
                        values = values[:len(column_names)]

                    # Create dictionary
                    record = dict(zip(column_names, values))

                    await self.producer.send(topic_name, value=record)

                    idx += 1
                    if idx % rate_limit == 0:
                        print(f"  Sent {idx} records")
        
        # Ensure all messages are sent
        await self.producer.flush()
        print(f"✓ Completed streaming {table_name} ({idx} records)\n")
    
    async def stream_all_tables(self, rate_limit=10000):
        """
        Automatically discover and stream ALL tables
        Based on table_columns keys
        """
        print(f"Found {len(table_columns)} tables to stream\n")
        
        for table_name in table_columns.keys():
            try:
                await self.stream_table(table_name, rate_limit)
            except Exception as e:
                print(f"✗ Error streaming {table_name}: {e}\n")
    
    async def close(self):
        if self.producer:
            await self.producer.stop()

async def main():
    parser = argparse.ArgumentParser(description='Stream TPC-H tables to Kafka using aiokafka.')
    parser.add_argument('--data-dir','-d', type=str, default='./tpch_data', help='Path to the TPC-H data files')
    parser.add_argument('--bootstrap-servers', '-b ', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--rate-limit', type=int, default=1000, help='Rate limit for streaming')

    args = parser.parse_args()

    data_path = args.data_dir
    bootstrap_servers = args.bootstrap_servers

    producer = TPCHAsyncKafkaProducer(data_path, bootstrap_servers)
    await producer.start()
    try:
        # await producer.stream_all_tables(args.rate_limit)
        await producer.stream_table('lineitem', args.rate_limit)
    finally:
        await producer.close()

if __name__ == "__main__":
    asyncio.run(main())

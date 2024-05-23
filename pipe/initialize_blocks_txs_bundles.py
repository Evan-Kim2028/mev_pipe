import os
import polars as pl
import requests
import time


import asyncio
import hypersync

from dotenv import load_dotenv
from lancedb_tables.lance_table import LanceTable


from hypersync import ColumnMapping, DataType, TransactionField, BlockField

# Load environment variables from .env file
load_dotenv()


def fetch_libmev_bundles(timestamp_start, timestamp_end, limit=100, offset=0, order_by="block_number"):
    """
    Fetch data from the libmev API within the specified timestamp range.

    Parameters:
    - timestamp_start (int): Start of the timestamp range.
    - timestamp_end (int): End of the timestamp range.
    - tags (list): List of tags to filter the data.
    - limit (int): Maximum number of records to fetch per request.
    - offset (int): Offset for pagination.
    - order_by (str): Field to order the results by.

    Returns:
    - dict: JSON response from the API if successful, None otherwise.

    Notes:
    - The function uses an exponential backoff strategy to handle rate limit errors (HTTP 429).
    - If the task fails due to rate limiting, it will retry up to 10 times with exponentially increasing delays.
    - Prefect will automatically retry the task up to 5 times with a 10-second delay between retries if the task ultimately fails.
    """
    # get endpoint from .env file
    endpoint = os.getenv("LIBMEV_ENDPOINT")
    params = {
        "timestampRange": f"{timestamp_start},{timestamp_end}",
        "filterByTags": ",".join(["naked_arb", "backrun", "sandwich", "liquidation"]),
        "limit": limit,
        "offset": offset,
        "orderByDesc": order_by,
    }

    retries = 10
    for i in range(retries):
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            # Additive backoff strategy, wait extra 15 seconds every time.
            wait_time = 15 * i
            print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Failed to retrieve data: {response.status_code}")
            return None
    print("Exceeded maximum retries")
    raise Exception("Exceeded maximum retries due to rate limiting")


def fetch_paginated_data(timestamp_start, timestamp_end, limit=100, pages=1000) -> pl.DataFrame:
    """
    ! TODO - there is currently a bug where the last iteration of data throws an error:
    Traceback (most recent call last):
  File "/home/evan/Documents/mev_pipe/pipe/blocks_txs_bundles.py", line 220, in <module>
    asyncio.run(historical_blocks_txs_sync())
  File "/home/evan/.rye/py/cpython@3.12.2/install/lib/python3.12/asyncio/runners.py", line 194, in run
    return runner.run(main)
           ^^^^^^^^^^^^^^^^
  File "/home/evan/.rye/py/cpython@3.12.2/install/lib/python3.12/asyncio/runners.py", line 118, in run
    return self._loop.run_until_complete(task)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/evan/.rye/py/cpython@3.12.2/install/lib/python3.12/asyncio/base_events.py", line 685, in run_until_complete
    return future.result()
           ^^^^^^^^^^^^^^^
  File "/home/evan/Documents/mev_pipe/pipe/blocks_txs_bundles.py", line 214, in historical_blocks_txs_sync
    lance_tables.write_table(uri="data", table="libmev_bundles", data=bundle_df, merge_on="block_number"
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/lancedb_tables/lance_table.py", line 59, in write_table
    .execute(data)
     ^^^^^^^^^^^^^
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/lancedb/merge.py", line 107, in execute
    self._table._do_merge(self, new_data, on_bad_vectors, fill_value)
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/lancedb/table.py", line 1644, in _do_merge
    new_data = _sanitize_data(
               ^^^^^^^^^^^^^^^
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/lancedb/table.py", line 125, in _sanitize_data
    data = _sanitize_schema(
           ^^^^^^^^^^^^^^^^^
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/lancedb/table.py", line 1766, in _sanitize_schema
    return pa.Table.from_arrays(
           ^^^^^^^^^^^^^^^^^^^^^
  File "pyarrow/table.pxi", line 3974, in pyarrow.lib.Table.from_arrays
  File "pyarrow/table.pxi", line 1464, in pyarrow.lib._sanitize_arrays
  File "pyarrow/array.pxi", line 371, in pyarrow.lib.asarray
  File "pyarrow/table.pxi", line 566, in pyarrow.lib.ChunkedArray.cast
  File "/home/evan/Documents/mev_pipe/.venv/lib/python3.12/site-packages/pyarrow/compute.py", line 404, in cast
    return call_function("cast", [arr], options, memory_pool)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "pyarrow/_compute.pyx", line 590, in pyarrow._compute.call_function
  File "pyarrow/_compute.pyx", line 385, in pyarrow._compute.Function.call
  File "pyarrow/error.pxi", line 154, in pyarrow.lib.pyarrow_internal_check_status
  File "pyarrow/error.pxi", line 91, in pyarrow.lib.check_status
pyarrow.lib.ArrowTypeError: struct fields don't match or are in the wrong order: 


    Fetch paginated data from the libmev API by calling fetch_libmev_bundles repeatedly.

    Parameters:
    - timestamp_start (int): Start of the timestamp range.
    - timestamp_end (int): End of the timestamp range.
    - tags (list): List of tags to filter the data.
    - limit (int): Maximum number of records to fetch per request.
    - pages (int): Number of pages to fetch.

    Returns:
    - list: Aggregated list of data fetched from the API.
    """
    all_data = []
    total_rows = 0

    for page in range(pages):
        offset = page * limit
        data = fetch_libmev_bundles(
            timestamp_start, timestamp_end, limit=limit, offset=offset)

        fetched_data = data["data"]
        if len(fetched_data) == 0:
            print(f"Page {page + 1}: Fetched 0 rows, exiting the loop.")
            break  # Stop if 0 rows are fetched
        all_data.extend(fetched_data)
        total_rows += len(fetched_data)
        print(f"Page {page + 1}: Fetched {len(fetched_data)
                                          } rows, Total: {total_rows} rows")

    return pl.DataFrame(all_data)


async def historical_blocks_txs_sync():
    """
    Use hypersync to query blocks and transactions and write to a LanceDB table.
    """
    # hypersync client
    client = hypersync.HypersyncClient("https://eth.hypersync.xyz")

    # Write blocks and transactions data into lancedb tables.
    blocks_table_name = "blocks"
    txs_table_name = "transactions"
    index: str = "block_number"

    lance_tables = LanceTable()

    # set to_block and from_block to query the desired block range.
    block_height: int = await client.get_height()
    to_block = block_height - 64  # 2 epochs past
    # 8 days, because the 8th day crashes due to an error with libmev non-schema standardization
    from_block: int = to_block - (7200 * 8)

    # add +/-1 to the block range because the query is exclusive to the block number
    query = client.preset_query_blocks_and_transactions(
        from_block-1, to_block+1)
    # Setting this number lower reduces client sync console error messages.
    query.max_num_transactions = 1_000  # for troubleshooting

    # batch size for processing the hypersync query and writing table to lancedb
    db_batch_size: int = 7200

    while from_block < to_block:
        current_to_block = min(from_block + db_batch_size, to_block)
        print(
            f"Processing blocks {from_block} to {current_to_block}")

        # add +/-1 to the block range because the query is exclusive to the block number
        query = client.preset_query_blocks_and_transactions(
            from_block-1, current_to_block+1)
        # Setting this number lower reduces client sync console error messages.
        query.max_num_transactions = 500  # for troubleshooting

        config = hypersync.ParquetConfig(
            path="data",
            hex_output=True,
            batch_size=500,
            concurrency=10,
            retry=True,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                },
                block={
                    BlockField.GAS_LIMIT: DataType.FLOAT64,
                    BlockField.GAS_USED: DataType.FLOAT64,
                    BlockField.SIZE: DataType.FLOAT64,
                    BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                    BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64,
                    BlockField.TIMESTAMP: DataType.INT64,
                },
            )
        )

        await client.create_parquet_folder(query, config)
        from_block = current_to_block  # Update from_block for the next batch

        # load the dataframe into a polars dataframe, join, and insert into lance table
        blocks_df = pl.read_parquet(
            f"data/{blocks_table_name}.parquet").rename({'number': 'block_number'})
        txs_df = pl.read_parquet(f"data/{txs_table_name}.parquet")

        # Preprocessing transformations
        blocks_txs_df = (
            txs_df.join(
                blocks_df, on='block_number', how='left', suffix='_blocks').unique().with_columns(
                pl.col('block_number').cast(pl.Int64),
                pl.from_epoch('timestamp').alias('datetime')
            ).with_columns(
                (pl.col('max_priority_fee_per_gas') / 10 **
                 9).round(9).alias('max_priority_fee_per_gas_gwei'),
                (pl.col('max_fee_per_gas') / 10 **
                    9).round(9).alias('max_fee_per_gas_gwei'),
                (pl.col('gas_price') / 10**9).round(9).alias('gas_price_gwei'),
                (pl.col('cumulative_gas_used') / 10 **
                 9).round(9).alias('cumulative_gas_used_gwei'),
                (pl.col('effective_gas_price') / 10 **
                 9).round(9).alias('effective_gas_price_gwei'),
                (pl.col('gas_used_blocks') / 10 **
                    9).round(9).alias('gas_used_blocks_million'),
            )
        )
        # get the min and max timestamps in the dataset and query libmev bundles
        timestamp_start = blocks_txs_df.select('timestamp').min().item()
        timestamp_end = blocks_txs_df.select('timestamp').max().item()

        print(f'fetching bundle data for {timestamp_start} to {timestamp_end}')
        bundle_df = fetch_paginated_data(
            timestamp_start, timestamp_end, 100, 1000)

        lance_tables.write_table(uri="data", table=txs_table_name, data=blocks_txs_df, merge_on=index
                                 )

        # write bundles to table
        lance_tables.write_table(uri="data", table="libmev_bundles", data=bundle_df, merge_on="block_number"
                                 )


# run the script
start_time = time.time()
asyncio.run(historical_blocks_txs_sync())
end_time = time.time()

print(f"Time taken: {end_time - start_time}")

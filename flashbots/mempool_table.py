import polars as pl
import time
from datetime import datetime, timedelta

from lancedb_tables.lance_table import LanceTable

# BUILD LANCE TABLE FROM FLASHBOTS MEMPOOL


# TODO - need to make datetime dynamic
# Current date
current_date = datetime.now()

# End date is the day before the current date
end_date = current_date - timedelta(days=1)

# Start date is the day before the end date
start_date = end_date - timedelta(days=3)

# Base URL endpoint. Replace this with other tables to explore different datasets
base_url = "https://mempool-dumpster.flashbots.net/ethereum/mainnet/"

lance_tables = LanceTable()

start_time = time.time()

# Loop to generate endpoints and process each URL
for n in range((end_date - start_date).days + 1):
    date = start_date + timedelta(days=n)
    url = f"{base_url}{
        date.year}-{date.month:02d}/{date.year}-{date.month:02d}-{date.day:02d}.parquet"

    loop_time = time.time()
    df = pl.read_parquet(url)
    lance_tables.write_table(uri="data", table="mempool",
                             data=df, merge_on="timestamp")
    print(f"Time taken for {url}: {time.time() - loop_time}")

end_time = time.time()
print(f"Total time taken: {end_time - start_time}")

# Description
Fetch data from coinbase api, convert to parquet file and upload to s3
# Install Dependencies
Dependencies used: requests, boto3, pandas, pyarrow

From root directory run below command
```
pip install -r requirements.txt to install dependencies
```
# Run
From root directory run below command
```
python main.py  for currencies and SPOT price
python get_price_at_date.py for a specific date price
```


 In the [main.py](main.py), call the method `fetch_and_upload` with respective parameters. See method documentation to understand more. \
    For e.g. To fetch price of bitcoin by date 
    ```
    req_data = {'date': PRICES_SPOT_BITCOIN_DATE}
    fetch_and_upload(PRICES_SPOT_BITCOIN_PATH, "bitcoin_price_historical", req_data=req_data)
    ```

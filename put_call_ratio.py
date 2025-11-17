import httpx
import polars as pl
import yfinance as yf

def put_call_ratio():
    pass

def get_pcr() -> pl.DataFrame:
    df = pl.read_csv('spx_pcr.csv', separator=';')
    df = df.with_columns(
        pl.col("Date")
        .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S")
        .cast(pl.Datetime(time_unit='ns'))
        .dt.replace_time_zone("America/New_York")
    )

    return df

def get_price_data(ticker: str) -> pl.DataFrame:
    data = yf.Ticker(ticker).history(period='max', interval='1d')
    df = pl.from_pandas(data, include_index = True)
    return df

def main():
    pcr_df = get_pcr()
    price_df = get_price_data('SPY')
    merged_df = price_df.join(pcr_df, on='Date')
    print(merged_df)

if __name__ == '__main__':
    main()
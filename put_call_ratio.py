from dataclasses import dataclass
import httpx
import polars as pl
import yfinance as yf

pl.Config.set_tbl_rows(1000)   # show up to 100 rows



@dataclass
class Settings:
    LOW_PCR: float = 1.2
    HIGH_PCR: float = 2.0

    CAPITAL: int = 100_000

    
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

def generate_signal(df: pl.DataFrame) -> pl.DataFrame:
    signals = []
    for pcr in df['PCR']:
        if pcr < Settings.LOW_PCR:
            signals.append(1)
        elif pcr > Settings.HIGH_PCR:
            signals.append(-1)
        else:
            signals.append(0)
    
    df = df.with_columns(
        pl.Series("Signal", signals)
    )
    
    return df

def calculate_positions(df: pl.DataFrame) -> pl.DataFrame:
    
    positions = []
    for pcr, signal in zip(df['PCR'], df['Signal']):
        current = list(filter(lambda x: x!= 0, positions))
        if signal == 1:
            if len(positions) == 0:
                positions.append(signal)
            elif ([0] + list(filter(lambda x: x!= 0, positions)))[-1] != signal:
                positions.append(signal)
            else:
                positions.append(0)
        elif signal == -1:
            if ([0] + list(filter(lambda x: x!= 0, positions)))[-1] == 1:       
                positions.append(signal)
            else:
                positions.append(0)
        else:
            positions.append(0)
    
    df = df.with_columns(
        pl.Series("Positions", positions)
    )

    return df

def simulate_trades(df: pl.DataFrame) -> pl.DataFrame:
    data = df.to_dict()
    
    capital = Settings.CAPITAL
    cap_col, eq_col = [capital], [0]
    manipulator = {'cap': 0, 'eq': 0}
    qty = 0
    transaction = False
    for i, position in enumerate(data['Positions']):
        if not transaction:
            cap_col.append(cap_col[-1])
            if qty > 0:
                eq_col.append(qty * data['Close'][i])
            else:
                eq_col.append(eq_col[-1])
        
        transaction = False
        if position == 1:
            qty = cap_col[-1] / data['Open'][i + 1]
            eq_col.append(cap_col[-1])
            cap_col.append(0)
            transaction = True
        elif position == -1:
            eq_col.append(0)
            cap_col.append(qty * data['Open'][i + 1])
            qty = 0
            transaction = True
        

        
    
    df = df.with_columns(
        pl.Series('Capital', cap_col[1:], dtype=pl.Float64),
        pl.Series('Equity', eq_col[1:], dtype=pl.Float64)
    )

    return df

def main():
    pcr_df = get_pcr()
    price_df = get_price_data('SPY')
    merged_df = price_df.join(pcr_df, on='Date')
    df = generate_signal(merged_df)

    df = calculate_positions(df)
    df = simulate_trades(df)
    print(df)

if __name__ == '__main__':
    main()
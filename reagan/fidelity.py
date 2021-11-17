from reagan.subclass import Subclass
import datetime as dt
import pandas as pd


class Fidelity(Subclass):
    def __init__(self):

        # super().__init__()
        # self.base_url = self.get_parameter('/fidelity/base_url')
        
        # Swapping to hardcoded to avoid too many boto api calls
        self.base_url = 'https://screener.fidelity.com/ftgw/etf/downloadCSV.jhtml?symbol='

    def _add_zero_days(self, df):

        """
        Parameters
        ----------
        df: pandas dataframe, raw stock price history

        Output
        ------
        df: pandas dataframe, original data with 0 volume days added in

        Some tickers will experience no volume in a given day. When importing,
        these days are not included at all. This function adds in the missing
        days with zero volume.
        """

        # Convert index to datetime
        df.index = pd.to_datetime(df.date)

        # Start is the first day that stock price is recorded
        start = df.index[0]

        # If market is closed, last stock day is current day. Otherwise
        # it is previous day
        end = max([pd.Timestamp(dt.date.today() - dt.timedelta(1)), df.index[-1]])

        # Create datetime index with all business days (no weekends).
        # Holidays are not excluded, but are removed in CombineData
        all_days = pd.DatetimeIndex(start=start, end=end, freq="B")

        # Reset the index with the full set, and fill nulls (days that are
        # empty are days with 0 stock volume)
        df = df.reindex(all_days)
        df.index.name = "Date"
        df.fillna({"Volume": 0}, inplace=True)
        df = df.fillna(method="ffill")
        df.date = df.index
        return df

    def pull_data(self, symbol):

        url = self.base_url + symbol.upper()
        df = pd.read_csv(url, index_col="Date").iloc[:-18]
        df["Date"] = df.index
        df = df.reset_index(drop=True)
        df.columns = map(lambda x: x.lower(), df.columns)
        df["volume"] = df["volume"].astype(int)
        if df.empty:
            return df
        else:
            return self._add_zero_days(df)

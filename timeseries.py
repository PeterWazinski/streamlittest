from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any
from arrow import now
import pandas as pd
from pyparsing import col
from NMFhierarchy import NMFinstrumentation

# Assumes NMFinstrumentation and hub_connector are imported from your project

@dataclass
class Timeseries:

    instrumentation: NMFinstrumentation
    hub: Any  # Should be hub_connector
    days_back: int = 7
    value_keys: list = None
    values: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    dataframes: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def __post_init__(self):
        self.retrieve_timeseries(value_keys=self.value_keys)

    def retrieve_timeseries(self, value_keys=None):
        """
        For each value key in value_keys, retrieve measurement values for the last N days.
        Populates self.values as {value_key: [ {timestamp, value, ...}, ... ]}
        Also creates a DataFrame for each value key in self.dataframes: {value_key: pd.DataFrame}
        The DataFrame has timestamps (rounded to full seconds) as index and values as the column.
        Throws ValueError if any key in value_keys is not in instrumentation.value_keys.
        If value_keys is None, retrieves for all instrumentation.value_keys.
        """

        all_keys = getattr(self.instrumentation, 'value_keys', [])
        if value_keys is None:
            keys_to_retrieve = all_keys
        else:
            # Check for invalid keys
            invalid_keys = [k for k in value_keys if k not in all_keys]
            if invalid_keys:
                raise ValueError(f"Invalid value_keys: {invalid_keys} not found in instrumentation.value_keys")
            keys_to_retrieve = value_keys

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.days_back)
        self.start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.dataframes = {}

        for key in keys_to_retrieve:
            # Get the raw timeseries data
            values_list = self.get_timeseries_data(value_key=key, from_=self.start_str, to_=self.end_str)
            self.values[key] = values_list

            # Build DataFrame: index = rounded timestamp, column = value
            if values_list:
                records = []
                # Extract timestamp and value pairs, rounding timestamps to full seconds
                for entry in values_list:
                    ts = entry.get('timestamp')
                    val = entry.get('value')
                    if ts is not None and val is not None:
                        # Round timestamp to full seconds
                        try:
                            dt = pd.to_datetime(ts)
                            dt = dt.round('s')  # Round to full seconds
                            records.append((dt, val))
                        except Exception:
                            continue
                #print(f"Found {len(records)} records for key {key}")    
                if records:
                    df = pd.DataFrame(records, columns=['timestamp', key])
                    df = df.set_index('timestamp')
                    self.dataframes[key] = df
                else:
                    self.dataframes[key] = pd.DataFrame(columns=[key])
            else:
                self.dataframes[key] = pd.DataFrame(columns=[key])


    def get_timeseries_data(self, value_key: str, from_: str, to_: str) -> List[Dict[str, Any]]:
        """
        Retrieve the timeseries data for a specific value key.
        """

        # instrumentations/70/values/current
        cmd = f"instrumentations/{self.instrumentation.id}/values/{value_key}?from={from_}&to={to_}"
        #print(cmd)
        
        return self.hub.call_hub_pagination(cmd=cmd, response_key="data")  # Enable pagination for the hub call

    def get_grouped_value_keys(self) -> Dict[str, List[str]]:
        """
        Returns a dictionary grouping value keys by their age:
        - "0_24h": keys with at least one entry younger than 24 hours           
        - "24_72h": keys with no entry younger than 24 hours but at least one younger than 72 hours
        - "72h+": keys with no entry younger than 72 hours
        """

        now = self.end_str  # Use the end time as the current time for analysis
        keys_24_72 = []
        keys_72 = []
        keys_24 = []

        for key, df in self.dataframes.items():
            # Print statistics for this value key
            if df.empty: 
                keys_72.append(key)
                continue
            latest = df.index.max()

            # Use pd.to_datetime to ensure latest is a datetime
            latest_dt = pd.to_datetime(latest)
            age = pd.to_datetime(now) - latest_dt
            if age > timedelta(hours=72):
                keys_72.append(key)
            elif age > timedelta(hours=24):
                keys_24_72.append(key)
            else:
                keys_24.append(key)

        return {
            "0_24h": keys_24,
            "24_72h": keys_24_72,
            "72h+": keys_72
        }


    def analyse_instrument_data(self):
        """
        Prints:
        - all value keys where at least one entry is younger than 24 hours
        - all value keys where no entry is younger than 24 hours but at least one is younger than 72 hours
        - all value keys where no entry is younger than 72 hours
        Uses get_grouped_value_keys for grouping.
        """
        print(f"Analysing timeseries data between {self.start_str} and {self.end_str}")
        grouped = self.get_grouped_value_keys()
        print("Value keys with at least one entry younger than 24h:")
        for k in grouped["0_24h"]:
            print(f"  {k}")
        print("Value keys with no entry younger than 24h but at least one younger than 72h:")
        for k in grouped["24_72h"]:
            print(f"  {k}")
        print("Value keys with no entry younger than 72h:")
        for k in grouped["72h+"]:
            print(f"  {k}")

    def cycle_statistics(self) -> Dict[str, Any]:
        """
        Computes statistics for the first available timeseries in the instrumentation's dataframes.
        Returns a dictionary with:
        - column name
        - number of entries
        - first and last timestamps
        - time range
        - median and mode of intervals (in minutes)
        - regular and outlier cycles (intervals between timestamps, using IQR method)
        """
    
        series_key = self.dataframes.keys()
        if series_key:
            col = list(series_key)[0]  # Use the first series as the column name
        else:
            return None  # No data available

        df = self.dataframes.get(col)

        stats = dict()
        stats['value_key'] = col
        stats['num_entries'] = len(df)
        stats['first_timestamp'] = df.index.min()
        stats['last_timestamp'] = df.index.max()    
        stats['time_range'] = df.index.max() - df.index.min()

        now = pd.Timestamp.now(tz=df.index.tz) if hasattr(df.index, 'tz') and df.index.tz else pd.Timestamp.now()
        now = now.round('s')  # Round to full seconds

        age = now - stats['last_timestamp']
        stats['age_last_timestamp'] = age

        # Add a measurement value 0 for the current time 'now' to the DataFrame
        df.loc[now, col] = 0

        # intervals between consecutive timestamps
        diffs = df.index.to_series().diff().dropna()
        # Round intervals to full minutes
        diffs_rounded = diffs.dt.total_seconds().div(60).round().astype(int)

        stats['median_interval'] = diffs_rounded.median()
        stats['mode_interval'] = diffs_rounded.mode().iloc[0] if not diffs_rounded.mode().empty else None

        # IQR method for outliers
        q1 = diffs_rounded.quantile(0.25)
        q3 = diffs_rounded.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = diffs_rounded[(diffs_rounded < lower_bound) | (diffs_rounded > upper_bound)]
        upper_outliers = outliers[outliers > upper_bound]
        regular = diffs_rounded[(diffs_rounded >= lower_bound) & (diffs_rounded <= upper_bound)]
        stats["regular_cycles"] = regular
        stats["outlier_cycles"] = upper_outliers

        return stats


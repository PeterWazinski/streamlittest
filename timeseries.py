from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd

# Assumes NMFinstrumentation and hub_connector are imported from your project

@dataclass
class Timeseries:
            
    instrumentation: Any  # Should be NMFinstrumentation
    hub: Any  # Should be hub_connector
    days: int = 7
    values: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    dataframes: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def __post_init__(self):
        self.retrieve_timeseries()

    def retrieve_timeseries(self):
        """
        For each value key in the instrumentation, retrieve measurement values for the last N days.
        Populates self.values as {value_key: [ {timestamp, value, ...}, ... ]}
        Also creates a DataFrame for each value key in self.dataframes: {value_key: pd.DataFrame}
        The DataFrame has timestamps (rounded to full seconds) as index and values as the column.
        """

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.days)
        self.start_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.end_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        self.dataframes = {}

        for key in getattr(self.instrumentation, 'value_keys', []):  
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
        
        return self.hub.call_hub_pagination(cmd=cmd, next_key="data")  # Enable pagination for the hub call

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
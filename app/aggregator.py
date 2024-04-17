"""Payment Aggregator.

This module provides a class `Aggregator` for aggregating payment data.

"""

from datetime import datetime

import aiofiles
import bson
import pandas as pd


class Aggregator:
    """Class for aggregating payment data.

    This class contains methods for loading payment data from a BSON file,
    filtering the data based on specified time range and group type, and
    aggregating the filtered data.

    """

    async def aggregate_payments(
            self,
            dt_from: datetime.isoformat,
            dt_upto: datetime.isoformat,
            group_type: str,
    ) -> dict:
        """Aggregate payments within a specified time range.

        Args:
            dt_from (str): Start date and time in ISO format (YYYY-MM-DDTHH:MM:SS).
            dt_upto (str): End date and time in ISO format (YYYY-MM-DDTHH:MM:SS).
            group_type (str): Type of time grouping ('month', 'day', or 'hour').

        Returns:
            dict: A dictionary containing aggregated payment data.

        """

        data_collection = await self.load_data_from_bson_async()

        return self.filter_data_collection(
            data_collection,
            dt_from,
            dt_upto,
            group_type
        )

    async def load_data_from_bson_async(self) -> list:
        """Load payment data from a BSON file asynchronously.

        Returns:
            list: A list of decoded BSON documents representing payment data.

        """
        async with aiofiles.open('../dump/sampleDB/sample_collection.bson', mode='rb') as f:
            data = await f.read()
            return bson.decode_all(data)

    @staticmethod
    def filter_data_collection(data_collection, dt_from, dt_upto, group_type):
        """Filter and aggregate payment data based on time range and group type.

        Args:
            data_collection (list): List of payment data.
            dt_from (str): Start date and time in ISO format (YYYY-MM-DDTHH:MM:SS).
            dt_upto (str): End date and time in ISO format (YYYY-MM-DDTHH:MM:SS).
            group_type (str): Type of time grouping ('month', 'day', or 'hour').

        Returns:
            dict: A dictionary containing aggregated payment data.

        """
        if group_type in ('month', 'day', 'hour'):
            # Generates a range of dates based on the provided start and end dates, with the specified frequency.
            full_range_collection = pd.date_range(start=dt_from, end=dt_upto, freq=group_type[0])

            # Creates a DataFrame to represent the generated date range, with a column named 'dt'.
            df_full_range = pd.DataFrame({'dt': full_range_collection})

            # Converts the 'dt' column in the DataFrame to periods based on the specified time grouping.
            df_full_range['dt'] = df_full_range['dt'].dt.to_period(group_type[0])

            # Creates a DataFrame from the provided payment data.
            df_collection = pd.DataFrame(data_collection)

            # Filters the payment data to include only records within the specified time range.
            df_collection = df_collection[(df_collection['dt'] >= dt_from) & (df_collection['dt'] <= dt_upto)]

            # Converts the 'dt' column in the DataFrame to periods based on the specified time grouping.
            df_collection['dt'] = df_collection['dt'].dt.to_period(group_type[0])

            # Groups the filtered payment data by time periods and calculates the sum of payment values for each period.
            df_collection = df_collection.groupby('dt')['value'].sum()

            # Merges the generated date range DataFrame with the aggregated payment data using a left join.
            df_full_range = pd.merge(df_full_range, df_collection, on='dt', how='left')

            # Fills any missing payment values with 0 and converts them to integers.
            df_full_range['value'] = df_full_range['value'].fillna(0).astype(int)

            # Prepares the final dataset and corresponding labels for further processing.
            dataset = df_full_range['value'].tolist()
            labels = [idx.strftime('%Y-%m-%dT%H:%M:%S') for idx in df_full_range['dt']]

            return {'dataset': dataset, 'labels': labels}



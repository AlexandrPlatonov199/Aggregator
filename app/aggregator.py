from datetime import datetime

import aiofiles
import bson
import pandas as pd


class Aggregator:

    async def aggregate_payments(
            self,
            dt_from: datetime.isoformat,
            dt_upto: datetime.isoformat,
            group_type: str,
    ):
        data_collection = await self.load_data_from_bson_async()

        return self.filter_data_collection(
            data_collection,
            dt_from,
            dt_upto,
            group_type
        )

    async def load_data_from_bson_async(self):
        async with aiofiles.open('../dump/sampleDB/sample_collection.bson', mode='rb') as f:
            data = await f.read()
            return bson.decode_all(data)

    @staticmethod
    def filter_data_collection(data_collection, dt_from, dt_upto, group_type):
        if group_type in ('month', 'day', 'hour'):
            full_range_collection = pd.date_range(start=dt_from, end=dt_upto, freq=group_type[0])
            df_full_range = pd.DataFrame({'dt': full_range_collection})
            df_full_range['dt'] = df_full_range['dt'].dt.to_period(group_type[0])

            df_collection = pd.DataFrame(data_collection)
            df_collection = df_collection[(df_collection['dt'] >= dt_from) & (df_collection['dt'] <= dt_upto)]

            df_collection['dt'] = df_collection['dt'].dt.to_period(group_type[0])
            df_collection = df_collection.groupby('dt')['value'].sum()

            df_full_range = pd.merge(df_full_range, df_collection, on='dt', how='left')
            df_full_range['value'] = df_full_range['value'].fillna(0).astype(int)

            dataset = df_full_range['value'].tolist()
            labels = [idx.strftime('%Y-%m-%dT%H:%M:%S') for idx in df_full_range['dt']]

            return {'dataset': dataset, 'labels': labels}



import asyncio
from datetime import datetime

import aiofiles
import bson


class Aggregator:

    async def aggregate_payments(
            self,
            dt_from: datetime.isoformat,
            dt_upto: datetime.isoformat,
            group_type: str,
    ):
        data_collection = await self.load_data_from_bson_async()

    async def load_data_from_bson_async(self):
        async with aiofiles.open('../dump/sampleDB/sample_collection.bson', mode='rb') as f:
            data = await f.read()
            return bson.decode_all(data)




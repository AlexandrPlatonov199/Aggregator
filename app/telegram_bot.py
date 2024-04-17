"""Telegram Bot Handlers.

This module contains message handlers for a Telegram bot using aiogram library.
It handles user commands and processes messages by aggregating payment data.

"""

import asyncio

from aiogram import types

from aggregator import Aggregator
from config import dp


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    """Send a welcome message to the user when they start the conversation.

    Args:
        message (types.Message): The message object representing the user's message.

    """
    user_name = message.from_user.full_name
    await message.reply(f'Привет, {user_name}!')


@dp.message_handler()
async def process_message(message: types.Message):
    """Process the user's message and aggregate payment data.

    If the message contains valid data in the format of a dictionary,
    it aggregates payment data based on the provided date range and time grouping type.

    Args:
        message (types.Message): The message object representing the user's message.

    """
    print(f"Входные данные: {message.text}")
    try:
        data = eval(message.text)
        result = await Aggregator().aggregate_payments(
            dt_from=data['dt_from'],
            dt_upto=data['dt_upto'],
            group_type=data['group_type'],
            )
        print(result)
        await message.answer(str(result))
    except Exception as e:
        print(e)
        await message.answer('Невалидный запрос. Пример запроса:'
                            '{"dt_from": "2022-09-01T00:00:00",'
                            '"dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')

if __name__ == '__main__':
    asyncio.run(dp.start_polling())
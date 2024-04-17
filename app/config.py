import os
from aiogram import Bot
from aiogram import Dispatcher
from dotenv import load_dotenv

load_dotenv()

bot = Bot(token=os.getenv('BOT_TOKEN'))
dp = Dispatcher(bot)
# Aggregator for RLT

## Installation
Clone the repository:
```shell
git clone https://github.com/AlexandrPlatonov199/Aggregator.git
```

Create and activate a virtual environment:
```shell
python -m venv .venv

.venv\Scripts\activate - для Windows;

source .venv/bin/activate - для Linux и MacOS.
```


Install the dependencies:

```shell
pip install poetry
```
```shell
poetry install 
```

Copy for .env and insert your bot’s token into BOT_TOKEN
```shell
cp .env.example .env
```

## Usage
Run:
```shell
cd app
```

```shell
python -m telegram_bot
```

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Divestiture Application
@author: Jack Kirby Cook

"""

import sys
import logging
import warnings
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
TICKERS = RESOURCES / "tickers.txt"

from alpaca.portfolio import AlpacaPortfolioDownloader
from finance.brokers import Authenticator, Brokerage
from finance.variables import Enumerations
from webscraping.webreaders import WebReader

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, term, tenure, **kwargs):
    brokerage = Brokerage(Enumerations.Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]
    intent = Enumerations.Intents.CLOSE

    with WebReader(delay=1) as source:
        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)

        portfolio = portfolio_downloader()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters.update({"term": Enumerations.Terms.LIMIT, "tenure": Enumerations.Tenure.DAY})
    parameters.update()
    main(*arguments, **parameters)

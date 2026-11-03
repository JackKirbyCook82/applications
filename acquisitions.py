# -*- coding: utf-8 -*-
"""
Created on Sun Oct 4 2026
@name:   Trading Acquisition Application
@author: Jack Kirby Cook
@file:   applications/acquisitions.py

"""

import sys
import logging
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime as Datetime

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"

from ibkr.website import IbkrSocket, IbkrSubscription
from ibkr.contracts import IbkrContractDownloader
from finance.querys import Symbol

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, ticker, expires, zscore, interest, **kwargs):
    product = Symbol(ticker)
    today = Datetime.today().date()
    with IbkrSocket(host="127.0.0.1", port=7497, delay=1) as source:
        contract_downloader = IbkrContractDownloader(name="ContractDownloader", source=source, today=today)
        with IbkrSubscription(source, product=product) as subscription:
            stock = subscription[product]
            product["spot"] = stock.price.trade
            product["volatility"] = stock.volatility.implied
            product["dividend"] = np.log1p(stock.dividends.predicted / stock.price.trade)
        contracts = contract_downloader(product, expires=expires, today=today, zscore=zscore, interest=interest)




if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)


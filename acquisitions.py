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
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
ACCOUNTS = RESOURCES / "accounts.txt"

from finance.brokers import Account, Brokerage
from finance.enumerations import Website

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, **kwargs):
    brokerage = Brokerage(Website.IKBR, False)
    account = Account.load(ACCOUNTS)[brokerage]


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)


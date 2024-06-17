# -*- coding: utf-8 -*-
"""
Created on Weds Jul 12 2023
@name:   ETrade Trading Platform Downloader
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import PySimpleGUI as gui
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
PROJECT = os.path.abspath(os.path.join(MAIN, os.pardir))
ROOT = os.path.abspath(os.path.join(PROJECT, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.market import ETradeContractDownloader, ETradeMarketDownloader
from finance.variables import DateRange, Querys
from finance.securities import SecurityFiles
from webscraping.webreaders import WebAuthorizer, WebReader
from support.files import Saver, FileTypes, FileTimings
from support.queues import Schedule, Scheduler, Queues
from support.synchronize import SideThread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


authorize = "https://us.etrade.com/e/t/etws/authorize?key={}&token={}"
request = "https://api.etrade.com/oauth/request_token"
access = "https://api.etrade.com/oauth/access_token"
base = "https://api.etrade.com"


class ETradeAuthorizer(WebAuthorizer, authorize=authorize, request=request, access=access, base=base): pass
class ETradeReader(WebReader, delay=10): pass
class SymbolQueue(Queues.FIFO, query=Querys.Symbol): pass
class ContractQueue(Queues.FIFO, query=Querys.Contract): pass


def contracts(*args, source, destination, reader, expires, parameters={}, **kwargs):
    contract_schedule = Schedule(name="MarketContractSchedule", source=source)
    contract_downloader = ETradeContractDownloader(name="MarketContractDownloader", feed=reader)
    contract_scheduler = Scheduler(name="MarketContractScheduler", destination=destination)
    contract_pipeline = contract_schedule + contract_downloader + contract_scheduler
    contract_thread = SideThread(contract_pipeline, name="MarketContractThread")
    contract_thread.setup(expires=expires, **parameters)
    return contract_thread


def security(*args, source, destination, reader, parameters={}, **kwargs):
    security_schedule = Schedule(name="MarketSecuritySchedule", source=source)
    security_downloader = ETradeMarketDownloader(name="MarketSecurityDownloader", feed=reader)
    security_saver = Saver(name="MarketSecuritySaver", destination=destination)
    security_pipeline = security_schedule + security_downloader + security_saver
    security_thread = SideThread(security_pipeline, name="MarketSecurityThread")
    security_thread.setup(**parameters)
    return security_thread


def main(*args, apikey, apicode, tickers, **kwargs):
    symbol_queue = SymbolQueue(name="SymbolQueue", querys=list(tickers), capacity=None)
    contract_queue = ContractQueue(name="ContractQueue", querys=[], capacity=None)
    option_file = SecurityFiles.Options(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    security_authorizer = ETradeAuthorizer(name="SecurityAuthorizer", apikey=apikey, apicode=apicode)
    with ETradeReader(name="SecurityReader", authorizer=security_authorizer) as security_reader:
        contract_parameters = dict(source=symbol_queue, destination=contract_queue, reader=security_reader)
        contract_thread = contracts(*args, **contract_parameters, **kwargs)
        security_parameters = dict(source=contract_queue, destination={option_file: "w"}, reader=security_reader)
        security_thread = security(*args, **security_parameters, **kwargs)
        contract_thread.start()
        contract_thread.join()
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    gui.theme("DarkGrey11")
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:2]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=60)).date()])
    main(apikey=sysApiKey, apicode=sysApiCode, tickers=sysTickers, expires=sysExpires, parameters={})




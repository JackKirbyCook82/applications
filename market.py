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
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
MARKET = os.path.join(ROOT, "repository", "market")
TICKERS = os.path.join(ROOT, "applications", "tickers.txt")
API = os.path.join(ROOT, "applications", "api.txt")
if ROOT not in sys.path:
    sys.path.append(ROOT)

from etrade.market import ETradeContractDownloader, ETradeMarketDownloader
from finance.securities import SecurityFiles
from finance.variables import Variables, DateRange, Symbol
from webscraping.webreaders import WebAuthorizer, WebReader
from support.files import Saver, FileTypes, FileTimings
from support.queues import Dequeuer, Requeuer, Queues
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
class SymbolDequeuer(Dequeuer, query=Variables.Querys.SYMBOL): pass
class ContractRequeuer(Requeuer, query=Variables.Querys.CONTRACT): pass
class ContractDequeuer(Dequeuer, query=Variables.Querys.CONTRACT): pass
class ContractSaver(Saver, query=Variables.Querys.CONTRACT): pass


def contracts(*args, reader, source, destination, expires, parameters={}, **kwargs):
    contract_dequeuer = SymbolDequeuer(name="MarketContractDequeuer", source=source)
    contract_downloader = ETradeContractDownloader(name="MarketContractDownloader", feed=reader)
    contract_requeuer = ContractRequeuer(name="MarketContractRequeuer", destination=destination)
    contract_pipeline = contract_dequeuer + contract_downloader + contract_requeuer
    contract_thread = SideThread(contract_pipeline, name="MarketContractThread")
    contract_thread.setup(expires=expires, **parameters)
    return contract_thread


def security(*args, reader, source, saving, parameters={}, **kwargs):
    security_dequeuer = ContractDequeuer(name="MarketSecurityDequeuer", source=source)
    security_downloader = ETradeMarketDownloader(name="MarketSecurityDownloader", feed=reader)
    security_saver = ContractSaver(name="MarketSecuritySaver", destination=saving)
    security_pipeline = security_dequeuer + security_downloader + security_saver
    security_thread = SideThread(security_pipeline, name="MarketSecurityThread")
    security_thread.setup(**parameters)
    return security_thread


def main(*args, apikey, apicode, symbols=[], **kwargs):
    contract_queue = Queues.FIFO(name="ContractQueue", values=list(symbols), capacity=None)
    security_queue = Queues.FIFO(name="SecurityQueue", contents=[], capacity=None)
    option_file = SecurityFiles.Option(name="OptionFile", repository=MARKET, filetype=FileTypes.CSV, filetiming=FileTimings.EAGER)
    security_authorizer = ETradeAuthorizer(name="SecurityAuthorizer", apikey=apikey, apicode=apicode)

    with ETradeReader(name="SecurityReader", authorizer=security_authorizer) as security_reader:
        contract_parameters = dict(reader=security_reader, source=contract_queue, destination=security_queue)
        security_parameters = dict(reader=security_reader, source=security_queue, saving={option_file: "w"})

        contract_thread = contracts(*args, **contract_parameters, **kwargs)
        security_thread = security(*args, **security_parameters, **kwargs)

        contract_thread.start()
        contract_thread.join()
        security_thread.start()
        security_thread.join()


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    with open(API, "r") as apifile:
        sysApiKey, sysApiCode = [str(string).strip() for string in str(apifile.read()).split("\n")]
    with open(TICKERS, "r") as tickerfile:
        sysTickers = [str(string).strip().upper() for string in tickerfile.read().split("\n")][0:10]
        sysSymbols = [Symbol(ticker) for ticker in sysTickers]
    sysExpires = DateRange([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=60)).date()])
    sysParameters = dict()
    main(apikey=sysApiKey, apicode=sysApiCode, symbols=sysSymbols, expires=sysExpires, parameters=sysParameters)




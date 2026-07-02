# -*- coding: utf-8 -*-
"""
Created on Weds Mar 18 2026
@name:   Trading Application
@author: Jack Kirby Cook

"""

import os
import sys
import queue
import logging
import warnings
import numpy as np
import pandas as pd
from enum import Enum
from types import SimpleNamespace
from attr.converters import to_bool
from datetime import datetime as Datetime
from datetime import timedelta as Timedelta
from collections import namedtuple as ntuple

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))
REPOSITORY = os.path.join(ROOT, "repository")
RESOURCES = os.path.join(ROOT, "resources")
if ROOT not in sys.path: sys.path.append(ROOT)
AUTHENTICATORS = os.path.join(RESOURCES, "authenticators.txt")
ACCOUNTS = os.path.join(RESOURCES, "accounts.txt")
TICKERS = os.path.join(RESOURCES, "tickers.txt")

from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.history import AlpacaBarsDownloader
from alpaca.orders import AlpacaSpreadUploader
from stocks.technicals import TechnicalCalculator
from options.markets import MarketCalculator, SanityCalculator, SurvivalCalculator, ViabilityCalculator
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from options.variances import VarianceCalculator, StandardizingCalculator
from options.localizing import LocalizingCalculator, LocalizingVariables
from options.spreads import SpreadCalculator, SpreadMetrics
from options.prospects import ProspectCalculator, PriorityCalculator
from finance.variables import Enumerations, Querys
from webscraping.webreaders import WebReader
from support.custom import NumRange, DateRange
from support.surface import SurfaceCreator
from support.plotters import Plotter, Plot, Pallet

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


Website = Enum("Website", ["ETRADE", "ALPACA", "INTERACTIVE"])
Brokerage = ntuple("Brokerage", {"website": Website, "live": bool})


def load(file):
    key = lambda website, live: Brokerage(Website[str(website).upper()], to_bool(live))
    value = lambda header, body: SimpleNamespace(**dict(zip(header, body)))
    contents = [str(line).split(" ") for line in open(file, "r").read().splitlines()]
    mapping = {key(*line[:2]): value(contents[0][2:], line[2:]) for line in contents[1:]}
    return mapping


def merge(stocks, technicals):
    technicals = technicals[technicals["date"] <= pd.Timestamp.today()]
    technicals = technicals.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).last()
    technicals = technicals[["ticker", "date", "volatility", "trend"]]
    stocks = stocks.merge(technicals, on="ticker", how="left")
    return stocks


def main(*args, tickers, history, expires, strikes, period, interest, dividends, term, tenure, **kwargs):
    authenticators, accounts, symbols = load(AUTHENTICATORS), load(ACCOUNTS), queue.Queue()
    for ticker in tickers: symbols.put(Querys.Symbol(ticker))
    spreads = [Enumerations.Spread.FLY, Enumerations.Spread.CALENDAR]
    technicals = [Enumerations.Technical.STATS]
    variables = LocalizingVariables.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    calendar = SpreadMetrics.create(ratios={"gap": +0.50, "theta": -0.35, "vega": +0.00}, zscore=0.50, profit=0.00)
    fly = SpreadMetrics.create(ratios={"gap": +0.50, "theta": -0.25}, zscore=0.75, profit=0.00)
    metrics = dict(calendar=calendar, fly=fly)

    with WebReader(delay=1) as source:
        bars_downloader = AlpacaBarsDownloader(name="BarsDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticators[Website.ALPACA, False])
        technical_calculator = TechnicalCalculator(name="TechnicalCalculator", technicals=technicals)
        sanity_calculator = SanityCalculator(name="SanityCalculator")
        market_calculator = MarketCalculator(name="MarketCalculator")
        survival_calculator = SurvivalCalculator(name="SurvivalCalculator", size=5, money=NumRange(0.05, 0.50), tight=NumRange(0.05, 0.50), gridsize=10)
        viability_calculator = ViabilityCalculator(name="ViabilityCalculator", size=5, money=0.20, tight=0.20)
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tight=0.20)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        variance_calculator = VarianceCalculator(name="VarianceCalculator", neighbors=25, quantile=0.95, multiple=2.5)
        localizing_calculator = LocalizingCalculator(name="LocalizingCalculator", variables=variables, samples=35, overlap=0.80)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        standardizing_calculator = StandardizingCalculator(name="StandardizingCalculator", neighbors=25)
        spread_calculator = SpreadCalculator(name="SpreadCalculator", spreads=spreads, limit=1)
        prospect_calculator = ProspectCalculator(name="ProspectCalculator", metrics=metrics)
        priority_calculator = PriorityCalculator(name="PriorityCalculator")
        spread_uploader = AlpacaSpreadUploader(name="SpreadUploader", source=source, authenticator=authenticators[Website.ALPACA, False])
        option_plotter = Plotter(name="OptionPlotter", plotsize=6)
        option_plotter["survivals"] = Plot(title="survivals", labels=["tight", "money", "survive"])
        option_plotter["generalized"] = Plot(title="generalized", labels=["tau", "mae", "tiv"])
        option_plotter["localized"] = Plot(title="localized", labels=["tau", "mae", "tiv"])

        while not symbols.empty():
            symbol = symbols.get()
            bars = bars_downloader([symbol], history=history)
            stocks = stock_downloader([symbol])
            technicals = technical_calculator(bars, period=period)
            stock = merge(stocks, technicals).squeeze()
            stock["mean"] = (stock["bid"] * stock["demand"] + stock["ask"] * stock["supply"]) / (stock["demand"] + stock["supply"])
            stock["median"] = (stock["bid"] + stock["ask"]) / 2
            strikes = NumRange.create([stock["last"] * strikes.minimum, stock["last"] * strikes.maximum])
            contracts = contract_downloader([symbol], expires=expires, strikes=strikes)
            options = option_downloader(contracts)
            options["volatility"] = stock["volatility"]
            options["spot"] = stock["median"]
            options = market_calculator(options, inplace=True)
            options = sanity_calculator(options)
            survivals = survival_calculator(options)
            options = viability_calculator(options)
            options = forward_calculator(options, interest=interest, dividends=dividends)
            valuations = valuation_calculator(options, interest=interest, dividends=dividends, inplace=False)
            volatilities = volatility_calculator(options, interest=interest, dividends=dividends, inplace=False)
            greeks = greek_calculator(options, interest=interest, dividends=dividends, inplace=False)

            raise Exception()

#            surface = Pallet.Scatter(survivals, color="blue", thickness=30, columns=["tightness", "moneyness", "survival"])
#            option_plotter["survivals"].append(surface)
#            location = Pallet.Line({"tightness": 0.20, "moneyness": 0.20}, color="red", thickness=5, columns=["tightness", "moneyness", "survivals"])
#            option_plotter["survivals"].append(location)

            generalized = variance_calculator(options)
            surface = surface_creator(generalized, method="regression", smoothing=1/10, weights=None)
            surface = Pallet.Surface(surface, color="blue", gridsize=100, transparency=0.75)
            option_plotter["generalized"].append(surface)

            localizer = localizing_calculator(generalized)
            localized = next(localizer)
            surface = surface_creator(localized, method="regression", smoothing=1/10, weights=None)
            option_plotter["localized"].append(surface)
            localized = standardizing_calculator(localized, surface)
            spreads = spread_calculator(localized)
            spreads = prospect_calculator(spreads)
            spreads = priority_calculator(spreads)

            raise Exception()
            spread_uploader(spreads, term=term, tenure=tenure)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["tickers"] = open(TICKERS, "r").read().splitlines()
    parameters["expires"] = DateRange.create([(Datetime.today() + Timedelta(days=1)).date(), (Datetime.today() + Timedelta(weeks=52*1/12)).date()])
    parameters["history"] = DateRange.create([(Datetime.today() - Timedelta(weeks=52*2)).date(), (Datetime.today() - Timedelta(days=1)).date()])
    parameters["strikes"] = NumRange.create([0.95, 1.05])
    parameters.update({"period": 252, "interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    parameters.update({"term": Enumerations.Terms.LIMIT, "tenure": Enumerations.Tenure.DAY})
    main(*arguments, **parameters)




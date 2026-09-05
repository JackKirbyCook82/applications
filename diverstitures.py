# -*- coding: utf-8 -*-
"""
Created on Mon Jul 6 2026
@name:   Trading Divestitures Application
@author: Jack Kirby Cook
@file:   applications/diverstitures.py

"""

import sys
import logging
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import timedelta as Timedelta

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.append(str(ROOT))
REPOSITORY = ROOT / "repository"
RESOURCES = ROOT / "resources"
AUTHENTICATORS = RESOURCES / "authenticators.txt"
ACCOUNTS = RESOURCES / "accounts.txt"
ORDERS = REPOSITORY / "orders"

from solutions.options import OptionDownloading, OptionFiltering, OptionPricing, OptionValuing
from alpaca.market import AlpacaStockDownloader, AlpacaContractDownloader, AlpacaOptionDownloader
from alpaca.portfolio import AlpacaPortfolioDownloader
from alpaca.orders import AlpacaOrderUploader, AlpacaOrderFile
from options import OptionCalculator, SanityFilter, ViabilityFilter
from options.localizing import ProximityCalculator, LocalizingVariables
from options.variances import VarianceCalculator, VarianceScreener, VarianceStandardizer
from options.divestitures import DivestitureCalculator, DivestitureMetric
from options.volatility import VolatilityCalculator
from options.valuations import ValuationCalculator
from options.forwards import ForwardCalculator
from options.greeks import GreekCalculator
from options.prospects import ProspectSlippage, ProspectCosting
from finance.brokers import Authenticator, Brokerage
from finance.enumerations import Website, Terms, Tenure
from finance.querys import Symbol, Contract
from webscraping.webreaders import WebReader
from support.surface import SurfaceCreator
from support.custom import DateRange, NumberRange

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


def main(*args, expire, strike, term, tenure, interest, dividends, **kwargs):
    localizing = LocalizingVariables.create(radius=(0.05, 0.12, 0.01), window=(1, 3, 1), coverage=(3, 10), limit=45/365)
    slippage = ProspectSlippage(entry=0.25, exit=0.35)
    costing = ProspectCosting(slippage=slippage, commissions=0.65 / 100)
    divesting = DivestitureMetric(multiple=0.25, ratio=0.25, eager=True)
    valuing = dict(method="regression", smoothing=1/10, weights=None)
    brokerage = Brokerage(Website.ALPACA, False)
    authenticator = Authenticator.load(AUTHENTICATORS)[brokerage]

    with WebReader(delay=1) as source:
        portfolio_downloader = AlpacaPortfolioDownloader(name="PortfolioDownloader", source=source, authenticator=authenticator)
        stock_downloader = AlpacaStockDownloader(name="StockDownloader", source=source, authenticator=authenticator)
        contract_downloader = AlpacaContractDownloader(name="ContractDownloader", source=source, authenticator=authenticator)
        option_downloader = AlpacaOptionDownloader(name="OptionDownloader", source=source, authenticator=authenticator)
        sanity_filter = SanityFilter(name="SanityFilter", size=5)
        option_calculator = OptionCalculator(name="OptionCalculator")
        viability_filter = ViabilityFilter(name="ViabilityFilter", active=0.30, money=0.15, tight=0.15)
        volatility_calculator = VolatilityCalculator(name="VolatilityCalculator", low=1e-4, high=5.0, tol=1e-10, iters=100)
        valuation_calculator = ValuationCalculator(name="ValuationCalculator")
        greek_calculator = GreekCalculator(name="GreekCalculator")
        forward_calculator = ForwardCalculator(name="ForwardCalculator", samplesize=5, tightness=0.15)
        variance_calculator = VarianceCalculator(name="VarianceCalculator")
        variance_screener = VarianceScreener(name="VarianceScreener", neighbors=25, quantile=0.95, multiple=2.5)
        variance_standardizer = VarianceStandardizer(name="VarianceStandardizer", neighbors=25)
        surface_creator = SurfaceCreator(name="SurfaceCreator", columns="tau|mae|tiv", quantity=35, gridsize=100, samplesize=5)
        proximity_calculator = ProximityCalculator(name="ProximityCalculator", localizing=localizing, samples=35, overlap=0.80)
        divestiture_calculator = DivestitureCalculator(name="DivestitureCalculator", costing=costing, metric=divesting)
        order_uploader = AlpacaOrderUploader(name="AlpacaOrderUploader", source=source, authenticator=authenticator)
        orders_file = AlpacaOrderFile(name="AlpacaOrderFile", file=ORDERS)

        option_downloading = OptionDownloading(stocks=stock_downloader, contracts=contract_downloader, options=option_downloader)
        option_filtering = OptionFiltering(sanity=sanity_filter, options=option_calculator, viability=viability_filter)
        option_pricing = OptionPricing(volatility=volatility_calculator, greeks=greek_calculator, forward=forward_calculator, variance=variance_calculator)
        option_valuing = OptionValuing(screen=variance_screener, surface=surface_creator, standardize=variance_standardizer, valuation=valuation_calculator)

        portfolio = portfolio_downloader()
        orders = orders_file.load(mode="r")
        portfolio = portfolio.merge(orders["order", "asset", "spread"], keys=["asset"], how="left", validate="one_to_one")
        for ticker, holdings in portfolio.groupby("ticker"):
            symbol = Symbol(ticker)
            expires = expire(DateRange(holdings["expires"].to_list()))
            strikes = strike(NumberRange(holdings["strikes"].to_list()))
            options = option_downloading(symbol, expires=expires, strikes=strikes)
            options = option_filtering(options)
            options = option_pricing(options, interest=interest, dividends=dividends)
            for order, holding in holdings.groupby("order"):
                proximity = proximity_calculator(options, holding)
                proximity = holdings.merge(proximity, on=list(Contract), how="left", validate="many_to_one")
                proximity = option_valuing(proximity, interest=interest, dividends=dividends, **valuing)
                divestitures = divestiture_calculator(proximity)
                if not bool(divestitures): continue
                orders = order_uploader(divestitures, term=term, tenure=tenure)
                orders_file.save(orders, mode="a")
                return


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.max_rows", 50)
    pd.set_option("display.width", 250)
    arguments, parameters = list(), dict()
    parameters["expire"] = lambda expires: DateRange(expires.minimum + Timedelta(weeks=-5), expires.maximum + Timedelta(weeks=+5))
    parameters["strike"] = lambda strikes: NumberRange(0.95 * strikes.minimum, 1.05 * strikes.maximum)
    parameters.update({"term": Terms.LIMIT, "tenure": Tenure.DAY})
    parameters.update({"interest": np.log10(1 + 0.05), "dividends": np.log10(1 + 0.00)})
    main(*arguments, **parameters)




# -*- coding: utf-8 -*-
"""
Created on Webs May 7 2025
@name:   Option Calculations
@author: Jack Kirby Cook

"""

import os
import sys
import logging
import warnings
import numpy as np
import xarray as xr
from abc import ABC, ABCMeta
from scipy.stats import norm
from datetime import date as Date
from datetime import timedelta as Timedelta

MAIN = os.path.dirname(os.path.realpath(__file__))
ROOT = os.path.abspath(os.path.join(MAIN, os.pardir))

from finance.variables import Variables
from support.calculations import Calculation, Equation, Variable
from support.visualize import Figure, Axes, Coordinate, Plot
from support.meta import RegistryMeta
from support.mixins import Naming

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class OptionEquation(Equation, ABC, datatype=xr.DataArray, vectorize=True):
    Γ = Variable.Dependent("Γ", "gamma", np.float32, function=lambda zx, xo, σ, q, τ: norm.pdf(+zx) / np.exp(q * τ) / np.sqrt(τ) / xo / σ)
    v = Variable.Dependent("v", "vega", np.float32, function=lambda zx, xo, σ, q, τ: norm.pdf(+zx) * np.sqrt(τ) * xo / np.exp(q * τ) / 100)
    τ = Variable.Dependent("τ", "tau", np.float32, function=lambda to, t: (np.datetime64(t, "ns") - np.datetime64(to, "ns")) / np.timedelta64(364, 'D'))

    zx = Variable.Dependent("zx", "underlying", np.float32, function=lambda zxk, zvt, zrt, zqt: zxk + zvt + zrt + zqt)
    zk = Variable.Dependent("zx", "strike", np.float32, function=lambda zxk, zvt, zrt, zqt: zxk - zvt + zrt + zqt)

    zxk = Variable.Dependent("zxk", "strike", np.float32, function=lambda xo, k, σ, τ: np.log(xo / k) / np.sqrt(τ) / σ)
    zvt = Variable.Dependent("zvt", "volatility", np.float32, function=lambda xo, σ, τ: np.sqrt(τ) * σ / 2)
    zrt = Variable.Dependent("zrt", "interest", np.float32, function=lambda xo, σ, r, τ: np.sqrt(τ) * r / σ)
    zqt = Variable.Dependent("zqt", "dividend", np.float32, function=lambda xo, σ, q, τ: np.sqrt(τ) * q / σ)

    xo = Variable.Independent("xo", "underlying", np.float32, locator="underlying")
    to = Variable.Independent("to", "current", np.datetime64, locator="current")

    σ = Variable.Independent("σ", "volatility", np.float32, locator="volatility")
    r = Variable.Independent("r", "interest", np.float32, locator="interest")
    q = Variable.Independent("q", "dividend", np.float32, locator="dividend")
    k = Variable.Independent("k", "strike", np.float32, locator="strike")
    t = Variable.Independent("t", "expire", np.datetime64, locator="expire")
    s = Variable.Independent("s", "ticker", str, locator="ticker")

class CallEquation(OptionEquation):
    yx = Variable.Dependent("yx", "underlying", np.float32, function=lambda zx, xo, q, τ: norm.cdf(+zx) * xo / np.exp(q * τ))
    yk = Variable.Dependent("yk", "strike", np.float32, function=lambda zk, k, r, τ: norm.cdf(+zk) * k / np.exp(r * τ))
    yτ = Variable.Dependent("yτ", "payoff", np.float32, function=lambda xo, k: np.maximum(xo - k, 0))
    yo = Variable.Dependent("yo", "valuation", np.float32, function=lambda yx, yk: + yx - yk)

    Θ = Variable.Dependent("Θ", "theta", np.float32, function=lambda zx, yx, yk, σ, q, r, τ: (+ yx * q - yk * r - norm.pdf(+zx) * σ / np.exp(q * τ) / np.sqrt(τ) / 2) / 364)
    ρ = Variable.Dependent("ρ", "rho", np.float32, function=lambda zk, k, r, τ: - norm.cdf(+zk) * k * τ / np.exp(r * τ) / 100)
    Δ = Variable.Dependent("Δ", "delta", np.float32, function=lambda zx, q, τ: + norm.cdf(+zx) / np.exp(q * τ))

class PutEquation(OptionEquation):
    yx = Variable.Dependent("yx", "underlying", np.float32, function=lambda zx, xo, q, τ: norm.cdf(-zx) * xo / np.exp(q * τ))
    yk = Variable.Dependent("yk", "strike", np.float32, function=lambda zk, k, r, τ: norm.cdf(-zk) * k / np.exp(r * τ))
    yτ = Variable.Dependent("yτ", "payoff", np.float32, function=lambda xo, k: np.maximum(k - xo, 0))
    yo = Variable.Dependent("yo", "valuation", np.float32, function=lambda yx, yk: - yx + yk)

    Θ = Variable.Dependent("Θ", "theta", np.float32, function=lambda zx, yx, yk, σ, q, r, τ: (- yx * q + yk * r - norm.pdf(+zx) * σ / np.exp(q * τ) / np.sqrt(τ) / 2) / 364)
    ρ = Variable.Dependent("ρ", "rho", np.float32, function=lambda zk, k, r, τ: + norm.cdf(-zk) * k * τ / np.exp(r * τ) / 100)
    Δ = Variable.Dependent("Δ", "delta", np.float32, function=lambda zx, q, τ: - norm.cdf(-zx) / np.exp(q * τ))


class OptionCalculation(Calculation, ABC, metaclass=RegistryMeta):
    def execute(self, *args, **kwargs):
        with self.equation(*args, **kwargs) as equation:
            yield equation.yo()
            yield equation.yτ()
            yield equation.t()
            yield equation.s()
            yield equation.k()

class CallCalculation(OptionCalculation, equation=CallEquation, register=Variables.Securities.Option.CALL): pass
class PutCalculation(OptionCalculation, equation=PutEquation, register=Variables.Securities.Option.PUT): pass


class OptionMeta(RegistryMeta, ABCMeta): pass
class Option(Naming, ABC, fields=["ticker", "expire", "strike", "interest", "dividend", "volatility"], metaclass=OptionMeta):
    def __call__(self, current, underlying):
        current = xr.DataArray(current, dims=["current"], coords={"current": current}, name="current")
        underlying = xr.DataArray(underlying, dims=["underlying"], coords={"underlying": underlying}, name="underlying")
        dataset = xr.Dataset({"current": current, "underlying": underlying})
        dataset.update(dict(self))
        return dataset

class CallOption(Option, register=Variables.Securities.Option.CALL): pass
class PutOption(Option, register=Variables.Securities.Option.PUT): pass


def main(*args, **kwargs):
    current = Date.today()
    expire = current + Timedelta(weeks=52)
    current = np.arange(current, expire, Timedelta(weeks=1)).astype(np.datetime64)
    underlying = np.arange(0, 200, 10)[1:]
    call = CallOption(ticker="AMD", expire=expire, strike=100, interest=0.00, dividend=0.00, volatility=0.50)
    calculation = CallCalculation(*args, **kwargs)
    array = call(current, underlying)
    array = calculation(array)
    figure = Figure(size=(12, 12), layout=(1, 1), name="Figure")

    print(array)


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="[%(levelname)s, %(threadName)s]:  %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
    warnings.filterwarnings("ignore")
    main()



"""
时间序列数据获取工具

一个用于从TimescaleDB数据库中获取时间序列数据的工具，
支持时间窗口分块、流式传输和多种输出格式。
"""

from .data_fetcher import TimeSeriesDataFetcher
from .config import TABLE_CONFIGS, DATABASE_CONFIG, DEFAULT_CONFIG

__version__ = "1.0.0"
__author__ = "Data Team"

__all__ = [
    "TimeSeriesDataFetcher",
    "TABLE_CONFIGS", 
    "DATABASE_CONFIG",
    "DEFAULT_CONFIG"
] 
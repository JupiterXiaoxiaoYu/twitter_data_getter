import os
from typing import Dict, List, Optional
from dotenv import load_dotenv

# 加载环境变量
load_dotenv("../kafka_to_timescaledb.env")

# 数据库连接配置
DATABASE_CONFIG = {
    "host": os.getenv("DB_HOST_PRIMARY", "localhost"),
    "port": int(os.getenv("DB_PORT_PRIMARY", "5434")),
    "database": os.getenv("DB_NAME_PRIMARY", "twitter_data"),
    "user": os.getenv("DB_USER_PRIMARY", "postgres"),
    "password": os.getenv("DB_PASSWORD_PRIMARY", "postgres"),
}

# 表配置：定义每个表的时间字段和主要字段
TABLE_CONFIGS = {
    "tweets": {
        "time_field": "created_at_ts",
        "primary_key": "tweet_id",
        "order_fields": ["created_at_ts", "tweet_id"],
        "description": "推文数据表"
    },
    "replies": {
        "time_field": "created_at_ts", 
        "primary_key": "tweet_id",
        "order_fields": ["created_at_ts", "tweet_id"],
        "description": "回复数据表"
    },
    "users": {
        "time_field": "updated_at",
        "primary_key": "user_id",
        "order_fields": ["updated_at", "user_id"],
        "description": "用户数据表"
    },
    "followers": {
        "time_field": "follower_created_at_ts",
        "primary_key": "follower_id",
        "order_fields": ["follower_created_at_ts", "user_id", "follower_id"],
        "description": "粉丝关系表"
    },
    "following": {
        "time_field": "following_created_at_ts",
        "primary_key": "following_id", 
        "order_fields": ["following_created_at_ts", "user_id", "following_id"],
        "description": "关注关系表"
    },
    "quoted_status_summary": {
        "time_field": "created_at_ts",
        "primary_key": "tweet_id",
        "order_fields": ["created_at_ts", "tweet_id"],
        "description": "引用推文摘要表"
    },
    "retweeted_status_summary": {
        "time_field": "created_at_ts",
        "primary_key": "tweet_id",
        "order_fields": ["created_at_ts", "tweet_id"],
        "description": "转发推文摘要表"
    },
    "kol_task_status": {
        "time_field": "updated_at",
        "primary_key": "user_id",
        "order_fields": ["updated_at", "user_id"],
        "description": "KOL任务状态表"
    }
}

# 默认配置
DEFAULT_CONFIG = {
    "chunk_size": 1000,  # 每次查询的记录数
    "time_interval_minutes": 60,  # 时间分块间隔（分钟）
    "output_format": "json",  # 输出格式：json, csv, parquet
    "include_metadata": True,  # 是否包含元数据
    "max_connections": 3,  # 最大数据库连接数
    "fetch_timeout": 300,  # 查询超时时间（秒）
}

# 支持的输出格式
SUPPORTED_FORMATS = ["json", "csv", "parquet", "jsonl"]

# 支持的时间格式
TIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S.%f"
]
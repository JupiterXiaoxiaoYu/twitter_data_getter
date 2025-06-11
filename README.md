# 时间序列数据获取工具

一个用于从TimescaleDB数据库中获取时间序列数据的工具，支持时间窗口分块、流式传输和多种输出格式。

## 功能特性

- ✅ **时间窗口分块**: 支持按时间段(t, t+1)自动分块处理大量数据
- ✅ **流式传输**: 异步流式处理，内存占用低
- ✅ **多种输出格式**: JSON, JSONL, CSV, Parquet
- ✅ **灵活筛选**: 支持自定义WHERE条件和字段选择
- ✅ **连接池管理**: 高效的数据库连接管理
- ✅ **进度监控**: 实时显示处理进度和统计信息
- ✅ **命令行工具**: 简单易用的CLI接口

## 支持的数据表

| 表名 | 描述 | 时间字段 |
|------|------|----------|
| `tweets` | 推文数据表 | `created_at_ts` |
| `replies` | 回复数据表 | `created_at_ts` |
| `users` | 用户数据表 | `updated_at` |
| `followers` | 粉丝关系表 | `follower_created_at_ts` |
| `following` | 关注关系表 | `following_created_at_ts` |
| `quoted_status_summary` | 引用推文摘要表 | `created_at_ts` |
| `retweeted_status_summary` | 转发推文摘要表 | `created_at_ts` |
| `kol_task_status` | KOL任务状态表 | `updated_at` |

## 安装依赖

```bash
pip install -r requirements.txt
```

## 环境配置

确保在上级目录存在 `kafka_to_timescaledb.env` 文件，包含数据库连接信息：

```env
DB_HOST_PRIMARY=localhost
DB_PORT_PRIMARY=5434
DB_NAME_PRIMARY=twitter_data
DB_USER_PRIMARY=postgres
DB_PASSWORD_PRIMARY=postgres
```

## 快速开始

### 1. 命令行使用

#### 列出支持的表
```bash
python cli.py list-tables
```

#### 统计数据数量
```bash
python cli.py count tweets --start-time "2024-01-01" --end-time "2024-01-02"
```

#### 流式获取数据
```bash
# 基本用法
python cli.py stream tweets --start-time "2024-01-01 00:00:00" --end-time "2024-01-01 23:59:59"

# 使用时间窗口分块（默认1小时窗口）
python cli.py stream tweets --start-time "2024-01-01" --end-time "2024-01-02" --time-interval 60

# 添加筛选条件
python cli.py stream tweets --start-time "2024-01-01" --end-time "2024-01-02" --where "likes > 100"

# 选择特定字段
python cli.py stream tweets --start-time "2024-01-01" --end-time "2024-01-02" --fields tweet_id user_id text likes
```

#### 导出数据到文件
```bash
# 导出为JSON
python cli.py export tweets --start-time "2024-01-01" --end-time "2024-01-02" -o tweets.json

# 导出为CSV
python cli.py export users --start-time "2024-01-01" --end-time "2024-01-02" -o users.csv --format csv

# 导出为JSONL（流式JSON）
python cli.py export replies --start-time "2024-01-01" --end-time "2024-01-02" -o replies.jsonl --format jsonl

# 导出为Parquet
python cli.py export tweets --start-time "2024-01-01" --end-time "2024-01-02" -o tweets.parquet --format parquet
```

### 2. Python API 使用

#### 基本示例

```python
import asyncio
from datetime import datetime, timedelta, timezone
from data_fetcher import TimeSeriesDataFetcher

async def main():
    # 创建数据获取器
    fetcher = TimeSeriesDataFetcher(
        chunk_size=1000,           # 每次获取1000条记录
        time_interval_minutes=60,  # 1小时时间窗口
        max_connections=3
    )
    
    try:
        # 初始化连接
        await fetcher.init_connection()
        
        # 定义时间范围
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=1)
        
        # 流式获取数据
        async for chunk in fetcher.stream_data_by_time_windows(
            table_name="tweets",
            start_time=start_time,
            end_time=end_time
        ):
            print(f"处理时间窗口 {chunk['window_index']}: {chunk['chunk_size']} 条记录")
            
            # 处理数据
            for record in chunk["data"]:
                # 你的数据处理逻辑
                pass
    
    finally:
        await fetcher.close_connection()

asyncio.run(main())
```

#### 带筛选条件的示例

```python
async def filtered_example():
    fetcher = TimeSeriesDataFetcher()
    
    try:
        await fetcher.init_connection()
        
        # 获取特定用户的高互动推文
        async for chunk in fetcher.stream_data_by_time_windows(
            table_name="tweets",
            start_time="2024-01-01",
            end_time="2024-01-02",
            where_conditions="user_id = '1234567890' AND likes > 100",
            select_fields=["tweet_id", "user_id", "text", "likes", "created_at_ts"]
        ):
            print(f"获取到 {chunk['chunk_size']} 条高互动推文")
            # 处理数据...
    
    finally:
        await fetcher.close_connection()
```

#### 导出文件示例

```python
async def export_example():
    fetcher = TimeSeriesDataFetcher()
    
    try:
        await fetcher.init_connection()
        
        # 导出最近7天的用户数据
        await fetcher.export_to_file(
            table_name="users",
            start_time=datetime.now(timezone.utc) - timedelta(days=7),
            end_time=datetime.now(timezone.utc),
            output_path="users_weekly.json",
            output_format="json",
            use_time_windows=True
        )
        
        print("用户数据导出完成!")
    
    finally:
        await fetcher.close_connection()
```

## API 文档

### TimeSeriesDataFetcher 类

#### 构造函数
```python
TimeSeriesDataFetcher(
    chunk_size: int = 1000,              # 每次查询的记录数
    time_interval_minutes: int = 60,     # 时间窗口间隔（分钟）
    max_connections: int = 3             # 最大数据库连接数
)
```

#### 主要方法

##### `stream_data_by_time_windows()`
按时间窗口流式获取数据

```python
async def stream_data_by_time_windows(
    table_name: str,                          # 表名
    start_time: Union[str, datetime],         # 开始时间
    end_time: Union[str, datetime],           # 结束时间
    where_conditions: Optional[str] = None,   # WHERE条件
    select_fields: Optional[List[str]] = None # 选择的字段
) -> AsyncGenerator[Dict, None]
```

##### `stream_data_by_chunks()`
按记录数分块流式获取数据（不按时间窗口）

```python
async def stream_data_by_chunks(
    table_name: str,
    start_time: Union[str, datetime],
    end_time: Union[str, datetime],
    where_conditions: Optional[str] = None,
    select_fields: Optional[List[str]] = None
) -> AsyncGenerator[Dict, None]
```

##### `export_to_file()`
导出数据到文件

```python
async def export_to_file(
    table_name: str,
    start_time: Union[str, datetime],
    end_time: Union[str, datetime],
    output_path: str,
    output_format: str = "json",              # json, jsonl, csv, parquet
    use_time_windows: bool = True,
    where_conditions: Optional[str] = None,
    select_fields: Optional[List[str]] = None
)
```

##### `get_table_count()`
获取指定条件下的记录总数

```python
async def get_table_count(
    table_name: str,
    start_time: Union[str, datetime],
    end_time: Union[str, datetime],
    where_conditions: Optional[str] = None
) -> int
```

## 输出格式

### 流式数据块结构

每个数据块包含以下字段：

```json
{
    "window_index": 1,                           // 时间窗口索引
    "window_start": "2024-01-01T00:00:00+00:00", // 窗口开始时间
    "window_end": "2024-01-01T01:00:00+00:00",   // 窗口结束时间
    "chunk_offset": 0,                           // 当前块的偏移量
    "chunk_size": 1000,                          // 当前块的记录数
    "total_records_so_far": 1000,                // 累计处理的记录数
    "data": [...],                               // 实际的数据记录
    "metadata": {
        "table_name": "tweets",
        "time_field": "created_at_ts",
        "query_time": "2024-01-01T12:00:00+00:00"
    }
}
```

### 支持的时间格式

- `YYYY-MM-DD`
- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DDTHH:MM:SS`
- `YYYY-MM-DDTHH:MM:SSZ`
- `YYYY-MM-DD HH:MM:SS.ffffff`

## 性能优化建议

### 1. 时间窗口大小
- **小窗口（5-15分钟）**: 适合实时处理，内存占用低
- **中窗口（30-60分钟）**: 适合常规数据处理
- **大窗口（2-4小时）**: 适合批量数据导出

### 2. 分块大小
- **小分块（100-500）**: 适合内存受限环境
- **中分块（1000-2000）**: 适合一般情况
- **大分块（5000+）**: 适合高性能环境

### 3. 连接池配置
- 根据数据库性能和并发需求调整 `max_connections`
- 推荐值：2-5个连接

## 常见问题

### Q: 如何处理大量数据而不占用过多内存？
A: 使用流式处理，设置较小的 `chunk_size`，并及时处理每个数据块。

### Q: 如何优化查询性能？
A: 
1. 合理设置时间窗口大小
2. 使用WHERE条件减少数据量
3. 只选择需要的字段
4. 确保时间字段上有索引

### Q: 支持并发处理吗？
A: 当前版本主要支持单线程流式处理，但可以在应用层实现并发处理多个时间窗口。

## 示例脚本

运行 `examples.py` 查看更多使用示例：

```bash
python examples.py
```

## 许可证

MIT License # twitter_data_getter

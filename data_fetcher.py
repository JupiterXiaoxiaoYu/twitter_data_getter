import asyncio
import asyncpg
import json
import csv
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, AsyncGenerator, Any, Union
from pathlib import Path
import logging
from io import StringIO

from config import DATABASE_CONFIG, TABLE_CONFIGS, DEFAULT_CONFIG

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TimeSeriesDataFetcher:
    """时间序列数据获取器，支持分块流式传输"""
    
    def __init__(self, 
                 chunk_size: int = None,
                 time_interval_minutes: int = None,
                 max_connections: int = None):
        """
        初始化数据获取器
        
        Args:
            chunk_size: 每次查询的记录数
            time_interval_minutes: 时间分块间隔（分钟）
            max_connections: 最大数据库连接数
        """
        self.chunk_size = chunk_size or DEFAULT_CONFIG["chunk_size"]
        self.time_interval_minutes = time_interval_minutes or DEFAULT_CONFIG["time_interval_minutes"]
        self.max_connections = max_connections or DEFAULT_CONFIG["max_connections"]
        self.pool = None
        
    async def init_connection(self):
        """初始化数据库连接池"""
        try:
            self.pool = await asyncpg.create_pool(
                **DATABASE_CONFIG,
                min_size=1,
                max_size=self.max_connections
            )
            logger.info("数据库连接池初始化成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    async def close_connection(self):
        """关闭数据库连接"""
        if self.pool:
            await self.pool.close()
            logger.info("数据库连接已关闭")
    
    def parse_time(self, time_str: str) -> datetime:
        """解析时间字符串为datetime对象"""
        from config import TIME_FORMATS
        
        if isinstance(time_str, datetime):
            return time_str
            
        for fmt in TIME_FORMATS:
            try:
                dt = datetime.strptime(time_str, fmt)
                # 确保时区感知
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        
        raise ValueError(f"无法解析时间格式: {time_str}")
    
    def generate_time_windows(self, 
                            start_time: Union[str, datetime], 
                            end_time: Union[str, datetime]) -> List[tuple]:
        """生成时间窗口列表"""
        start_dt = self.parse_time(start_time)
        end_dt = self.parse_time(end_time)
        
        windows = []
        current_time = start_dt
        
        while current_time < end_dt:
            window_end = min(
                current_time + timedelta(minutes=self.time_interval_minutes),
                end_dt
            )
            windows.append((current_time, window_end))
            current_time = window_end
            
        logger.info(f"生成了 {len(windows)} 个时间窗口")
        return windows
    
    async def get_table_count(self, 
                            table_name: str,
                            start_time: Union[str, datetime],
                            end_time: Union[str, datetime],
                            where_conditions: Optional[str] = None) -> int:
        """获取指定时间范围内的记录总数"""
        if table_name not in TABLE_CONFIGS:
            raise ValueError(f"不支持的表: {table_name}")
        
        config = TABLE_CONFIGS[table_name]
        time_field = config["time_field"]
        
        start_dt = self.parse_time(start_time)
        end_dt = self.parse_time(end_time)
        
        query = f"""
            SELECT COUNT(*) as total_count
            FROM {table_name}
            WHERE {time_field} >= $1 AND {time_field} < $2
        """
        
        if where_conditions:
            query += f" AND ({where_conditions})"
        
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(query, start_dt, end_dt)
            return result['total_count']
    
    async def fetch_data_chunk(self, 
                             table_name: str,
                             start_time: Union[str, datetime],
                             end_time: Union[str, datetime],
                             offset: int = 0,
                             limit: int = None,
                             where_conditions: Optional[str] = None,
                             select_fields: Optional[List[str]] = None) -> List[Dict]:
        """获取单个数据块"""
        if table_name not in TABLE_CONFIGS:
            raise ValueError(f"不支持的表: {table_name}")
        
        config = TABLE_CONFIGS[table_name]
        time_field = config["time_field"]
        order_fields = config["order_fields"]
        
        start_dt = self.parse_time(start_time)
        end_dt = self.parse_time(end_time)
        limit = limit or self.chunk_size
        
        # 构建SELECT字段
        fields = ", ".join(select_fields) if select_fields else "*"
        
        # 构建查询
        query = f"""
            SELECT {fields}
            FROM {table_name}
            WHERE {time_field} >= $1 AND {time_field} < $2
        """
        
        if where_conditions:
            query += f" AND ({where_conditions})"
        
        # 添加排序
        order_clause = ", ".join(order_fields)
        query += f" ORDER BY {order_clause}"
        
        # 添加分页
        query += f" LIMIT {limit} OFFSET {offset}"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, start_dt, end_dt)
            return [dict(row) for row in rows]
    
    async def stream_data_by_time_windows(self, 
                                        table_name: str,
                                        start_time: Union[str, datetime],
                                        end_time: Union[str, datetime],
                                        where_conditions: Optional[str] = None,
                                        select_fields: Optional[List[str]] = None) -> AsyncGenerator[Dict, None]:
        """按时间窗口流式获取数据"""
        windows = self.generate_time_windows(start_time, end_time)
        total_records = 0
        
        for i, (window_start, window_end) in enumerate(windows, 1):
            logger.info(f"处理时间窗口 {i}/{len(windows)}: {window_start} ~ {window_end}")
            
            # 获取当前窗口的记录数
            window_count = await self.get_table_count(
                table_name, window_start, window_end, where_conditions
            )
            
            if window_count == 0:
                logger.info(f"时间窗口 {i} 无数据，跳过")
                continue
            
            logger.info(f"时间窗口 {i} 共有 {window_count} 条记录")
            
            # 分块获取当前窗口的数据
            offset = 0
            window_records = 0
            
            while offset < window_count:
                chunk_data = await self.fetch_data_chunk(
                    table_name=table_name,
                    start_time=window_start,
                    end_time=window_end,
                    offset=offset,
                    limit=self.chunk_size,
                    where_conditions=where_conditions,
                    select_fields=select_fields
                )
                
                if not chunk_data:
                    break
                
                chunk_size = len(chunk_data)
                window_records += chunk_size
                total_records += chunk_size
                
                # 产出数据块
                yield {
                    "window_index": i,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "chunk_offset": offset,
                    "chunk_size": chunk_size,
                    "total_records_so_far": total_records,
                    "data": chunk_data,
                    "metadata": {
                        "table_name": table_name,
                        "time_field": TABLE_CONFIGS[table_name]["time_field"],
                        "query_time": datetime.now(timezone.utc).isoformat()
                    }
                }
                
                offset += self.chunk_size
                
                # 防止内存溢出，添加短暂休息
                if chunk_size >= self.chunk_size:
                    await asyncio.sleep(0.01)
            
            logger.info(f"时间窗口 {i} 完成，实际获取 {window_records} 条记录")
        
        logger.info(f"所有时间窗口处理完成，总计 {total_records} 条记录")
    
    async def stream_data_by_chunks(self, 
                                  table_name: str,
                                  start_time: Union[str, datetime],
                                  end_time: Union[str, datetime],
                                  where_conditions: Optional[str] = None,
                                  select_fields: Optional[List[str]] = None) -> AsyncGenerator[Dict, None]:
        """按记录数分块流式获取数据（不按时间窗口划分）"""
        total_count = await self.get_table_count(
            table_name, start_time, end_time, where_conditions
        )
        
        if total_count == 0:
            logger.info("没有找到符合条件的数据")
            return
        
        logger.info(f"总共需要获取 {total_count} 条记录")
        
        offset = 0
        chunk_index = 0
        
        while offset < total_count:
            chunk_data = await self.fetch_data_chunk(
                table_name=table_name,
                start_time=start_time,
                end_time=end_time,
                offset=offset,
                limit=self.chunk_size,
                where_conditions=where_conditions,
                select_fields=select_fields
            )
            
            if not chunk_data:
                break
            
            chunk_index += 1
            chunk_size = len(chunk_data)
            
            yield {
                "chunk_index": chunk_index,
                "chunk_offset": offset,
                "chunk_size": chunk_size,
                "total_count": total_count,
                "progress": (offset + chunk_size) / total_count,
                "data": chunk_data,
                "metadata": {
                    "table_name": table_name,
                    "time_field": TABLE_CONFIGS[table_name]["time_field"],
                    "query_time": datetime.now(timezone.utc).isoformat()
                }
            }
            
            offset += self.chunk_size
            await asyncio.sleep(0.01)  # 防止过度占用资源
        
        logger.info(f"数据获取完成，共 {chunk_index} 个块")
    
    def format_output(self, data: Dict, output_format: str) -> str:
        """格式化输出数据"""
        if output_format == "json":
            return json.dumps(data, ensure_ascii=False, indent=2, default=str)
        elif output_format == "jsonl":
            return json.dumps(data, ensure_ascii=False, default=str)
        elif output_format == "csv":
            if not data.get("data"):
                return ""
            
            # 将数据转换为CSV格式
            df = pd.DataFrame(data["data"])
            return df.to_csv(index=False)
        elif output_format == "parquet":
            # 这里返回文件路径，实际写入由调用者处理
            return "parquet"
        else:
            raise ValueError(f"不支持的输出格式: {output_format}")
    
    async def export_to_file(self, 
                           table_name: str,
                           start_time: Union[str, datetime],
                           end_time: Union[str, datetime],
                           output_path: str,
                           output_format: str = "json",
                           use_time_windows: bool = True,
                           where_conditions: Optional[str] = None,
                           select_fields: Optional[List[str]] = None):
        """导出数据到文件"""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if use_time_windows:
            data_stream = self.stream_data_by_time_windows(
                table_name, start_time, end_time, where_conditions, select_fields
            )
        else:
            data_stream = self.stream_data_by_chunks(
                table_name, start_time, end_time, where_conditions, select_fields
            )
        
        if output_format == "jsonl":
            # JSON Lines格式，每行一个JSON对象
            with open(output_path, 'w', encoding='utf-8') as f:
                async for chunk in data_stream:
                    f.write(json.dumps(chunk, ensure_ascii=False, default=str) + '\n')
        
        elif output_format == "json":
            # 完整JSON数组格式
            all_chunks = []
            async for chunk in data_stream:
                all_chunks.append(chunk)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_chunks, f, ensure_ascii=False, indent=2, default=str)
        
        elif output_format == "csv":
            # CSV格式，合并所有数据
            all_data = []
            async for chunk in data_stream:
                all_data.extend(chunk["data"])
            
            if all_data:
                df = pd.DataFrame(all_data)
                df.to_csv(output_path, index=False)
        
        elif output_format == "parquet":
            # Parquet格式
            all_data = []
            async for chunk in data_stream:
                all_data.extend(chunk["data"])
            
            if all_data:
                df = pd.DataFrame(all_data)
                df.to_parquet(output_path)
        
        logger.info(f"数据已导出到: {output_path}") 
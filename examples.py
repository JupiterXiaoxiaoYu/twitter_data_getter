 #!/usr/bin/env python3
"""
时间序列数据获取工具的使用示例
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone

from data_fetcher import TimeSeriesDataFetcher

async def example_basic_usage():
    """基本使用示例"""
    print("=== 基本使用示例 ===")
    
    # 创建数据获取器
    fetcher = TimeSeriesDataFetcher(
        chunk_size=100,           # 每次获取100条记录
        time_interval_minutes=30, # 30分钟时间窗口
        max_connections=2
    )
    
    try:
        # 初始化连接
        await fetcher.init_connection()
        
        # 定义时间范围（获取最近1天的数据）
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=1)
        
        print(f"获取时间范围: {start_time} ~ {end_time}")
        
        # 获取推文数据
        chunk_count = 0
        total_records = 0
        
        async for chunk in fetcher.stream_data_by_time_windows(
            table_name="tweets",
            start_time=start_time,
            end_time=end_time
        ):
            chunk_count += 1
            chunk_size = chunk["chunk_size"]
            total_records += chunk_size
            
            print(f"处理块 {chunk_count}: {chunk_size} 条记录")
            print(f"  时间窗口: {chunk['window_start']} ~ {chunk['window_end']}")
            print(f"  累计记录: {total_records}")
            
            # 处理数据（这里只是打印第一条记录的tweet_id）
            if chunk["data"]:
                first_record = chunk["data"][0]
                print(f"  示例记录ID: {first_record.get('tweet_id', 'N/A')}")
            
            print()
        
        print(f"获取完成: 总共 {chunk_count} 个块, {total_records} 条记录")
        
    finally:
        await fetcher.close_connection()

async def example_with_filters():
    """带筛选条件的示例"""
    print("\n=== 带筛选条件的示例 ===")
    
    fetcher = TimeSeriesDataFetcher(chunk_size=50)
    
    try:
        await fetcher.init_connection()
        
        # 获取最近12小时的数据
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=12)
        
        # 只获取特定用户的推文，且likes > 10
        where_conditions = "user_id = '1234567890' AND likes > 10"
        
        # 只选择特定字段
        select_fields = ["tweet_id", "user_id", "text", "created_at_ts", "likes", "retweets"]
        
        print(f"筛选条件: {where_conditions}")
        print(f"选择字段: {', '.join(select_fields)}")
        
        async for chunk in fetcher.stream_data_by_time_windows(
            table_name="tweets",
            start_time=start_time,
            end_time=end_time,
            where_conditions=where_conditions,
            select_fields=select_fields
        ):
            print(f"获取到 {chunk['chunk_size']} 条记录")
            
            # 打印前3条记录
            for i, record in enumerate(chunk["data"][:3]):
                print(f"  记录 {i+1}: {record}")
            
            if len(chunk["data"]) > 3:
                print(f"  ... 还有 {len(chunk['data']) - 3} 条记录")
        
    finally:
        await fetcher.close_connection()

async def example_export_to_file():
    """导出到文件的示例"""
    print("\n=== 导出到文件的示例 ===")
    
    fetcher = TimeSeriesDataFetcher(chunk_size=200)
    
    try:
        await fetcher.init_connection()
        
        # 获取最近6小时的用户数据
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=6)
        
        # 导出为JSON文件
        await fetcher.export_to_file(
            table_name="users",
            start_time=start_time,
            end_time=end_time,
            output_path="output/users_recent.json",
            output_format="json",
            use_time_windows=True
        )
        print("JSON导出完成: output/users_recent.json")
        
        # 导出为CSV文件
        await fetcher.export_to_file(
            table_name="users",
            start_time=start_time,
            end_time=end_time,
            output_path="output/users_recent.csv",
            output_format="csv",
            use_time_windows=False,  # 不使用时间窗口，直接按记录分块
            select_fields=["user_id", "user_name", "user_screen_name", "user_followers_count", "updated_at"]
        )
        print("CSV导出完成: output/users_recent.csv")
        
        # 导出为JSONL文件（流式JSON）
        await fetcher.export_to_file(
            table_name="users",
            start_time=start_time,
            end_time=end_time,
            output_path="output/users_recent.jsonl",
            output_format="jsonl",
            use_time_windows=True
        )
        print("JSONL导出完成: output/users_recent.jsonl")
        
    finally:
        await fetcher.close_connection()

async def example_multiple_tables():
    """多表数据获取示例"""
    print("\n=== 多表数据获取示例 ===")
    
    fetcher = TimeSeriesDataFetcher(chunk_size=100)
    
    try:
        await fetcher.init_connection()
        
        # 获取最近3小时的数据
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=3)
        
        tables_to_process = ["tweets", "replies", "users"]
        
        for table_name in tables_to_process:
            print(f"\n处理表: {table_name}")
            
            # 获取记录总数
            total_count = await fetcher.get_table_count(
                table_name=table_name,
                start_time=start_time,
                end_time=end_time
            )
            print(f"  总记录数: {total_count}")
            
            if total_count == 0:
                print("  无数据，跳过")
                continue
            
            # 流式获取数据
            processed_records = 0
            async for chunk in fetcher.stream_data_by_chunks(
                table_name=table_name,
                start_time=start_time,
                end_time=end_time
            ):
                processed_records += chunk["chunk_size"]
                progress = chunk["progress"]
                print(f"  处理进度: {progress:.1%} ({processed_records}/{total_count})")
                
                # 这里可以对数据进行处理
                # 例如：数据清洗、转换、分析等
                
            print(f"  表 {table_name} 处理完成")
        
    finally:
        await fetcher.close_connection()

async def example_real_time_style():
    """模拟实时数据获取的示例"""
    print("\n=== 模拟实时数据获取的示例 ===")
    
    fetcher = TimeSeriesDataFetcher(
        chunk_size=50,
        time_interval_minutes=5  # 5分钟时间窗口，模拟实时处理
    )
    
    try:
        await fetcher.init_connection()
        
        # 模拟处理最近1小时的数据，但按5分钟窗口分块
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)
        
        print(f"模拟实时处理: {start_time} ~ {end_time}")
        print("每5分钟一个时间窗口")
        
        async for chunk in fetcher.stream_data_by_time_windows(
            table_name="tweets",
            start_time=start_time,
            end_time=end_time
        ):
            window_start = chunk["window_start"]
            window_end = chunk["window_end"]
            chunk_size = chunk["chunk_size"]
            
            print(f"时间窗口 {chunk['window_index']}: {window_start} ~ {window_end}")
            print(f"  处理 {chunk_size} 条记录")
            
            if chunk["data"]:
                # 模拟数据处理延迟
                await asyncio.sleep(0.1)
                
                # 简单的数据统计
                likes_sum = sum(record.get("likes", 0) for record in chunk["data"])
                avg_likes = likes_sum / chunk_size if chunk_size > 0 else 0
                
                print(f"  平均点赞数: {avg_likes:.1f}")
            
            print()
        
        print("实时数据处理完成")
        
    finally:
        await fetcher.close_connection()

async def main():
    """运行所有示例"""
    print("时间序列数据获取工具 - 使用示例")
    print("=" * 50)
    
    try:
        await example_basic_usage()
        await example_with_filters()
        await example_export_to_file()
        await example_multiple_tables()
        await example_real_time_style()
        
        print("\n所有示例运行完成！")
        
    except Exception as e:
        print(f"示例运行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
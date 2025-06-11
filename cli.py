 #!/usr/bin/env python3
"""
时间序列数据获取工具的命令行接口
"""

import asyncio
import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from data_fetcher import TimeSeriesDataFetcher
from config import TABLE_CONFIGS, SUPPORTED_FORMATS

async def stream_data_command(args):
    """流式获取数据命令"""
    fetcher = TimeSeriesDataFetcher(
        chunk_size=args.chunk_size,
        time_interval_minutes=args.time_interval,
        max_connections=args.max_connections
    )
    
    try:
        await fetcher.init_connection()
        
        print(f"开始获取表 '{args.table}' 的数据...")
        print(f"时间范围: {args.start_time} ~ {args.end_time}")
        print(f"输出格式: {args.format}")
        
        if args.use_time_windows:
            data_stream = fetcher.stream_data_by_time_windows(
                table_name=args.table,
                start_time=args.start_time,
                end_time=args.end_time,
                where_conditions=args.where,
                select_fields=args.fields
            )
        else:
            data_stream = fetcher.stream_data_by_chunks(
                table_name=args.table,
                start_time=args.start_time,
                end_time=args.end_time,
                where_conditions=args.where,
                select_fields=args.fields
            )
        
        total_chunks = 0
        total_records = 0
        
        async for chunk in data_stream:
            total_chunks += 1
            chunk_size = chunk["chunk_size"]
            total_records += chunk_size
            
            if args.output:
                # 保存到文件
                pass  # 由export命令处理
            else:
                # 输出到控制台
                if args.format == "json":
                    output = fetcher.format_output(chunk, "json")
                    print(output)
                elif args.format == "jsonl":
                    output = fetcher.format_output(chunk, "jsonl")
                    print(output)
                else:
                    print(f"处理块 {total_chunks}: {chunk_size} 条记录")
                    if args.verbose:
                        print(f"  时间范围: {chunk.get('window_start', 'N/A')} ~ {chunk.get('window_end', 'N/A')}")
                        print(f"  累计记录: {total_records}")
        
        print(f"\n数据获取完成:")
        print(f"  总块数: {total_chunks}")
        print(f"  总记录数: {total_records}")
        
    finally:
        await fetcher.close_connection()

async def export_data_command(args):
    """导出数据命令"""
    fetcher = TimeSeriesDataFetcher(
        chunk_size=args.chunk_size,
        time_interval_minutes=args.time_interval,
        max_connections=args.max_connections
    )
    
    try:
        await fetcher.init_connection()
        
        print(f"开始导出表 '{args.table}' 的数据到文件...")
        print(f"时间范围: {args.start_time} ~ {args.end_time}")
        print(f"输出文件: {args.output}")
        print(f"输出格式: {args.format}")
        
        await fetcher.export_to_file(
            table_name=args.table,
            start_time=args.start_time,
            end_time=args.end_time,
            output_path=args.output,
            output_format=args.format,
            use_time_windows=args.use_time_windows,
            where_conditions=args.where,
            select_fields=args.fields
        )
        
        print("数据导出完成!")
        
    finally:
        await fetcher.close_connection()

async def count_data_command(args):
    """统计数据数量命令"""
    fetcher = TimeSeriesDataFetcher()
    
    try:
        await fetcher.init_connection()
        
        total_count = await fetcher.get_table_count(
            table_name=args.table,
            start_time=args.start_time,
            end_time=args.end_time,
            where_conditions=args.where
        )
        
        print(f"表 '{args.table}' 在指定时间范围内的记录数: {total_count}")
        
        if args.verbose:
            print(f"  时间范围: {args.start_time} ~ {args.end_time}")
            if args.where:
                print(f"  筛选条件: {args.where}")
        
    finally:
        await fetcher.close_connection()

def list_tables_command(args):
    """列出支持的表"""
    print("支持的数据表:")
    print("-" * 80)
    for table_name, config in TABLE_CONFIGS.items():
        print(f"表名: {table_name}")
        print(f"  描述: {config['description']}")
        print(f"  时间字段: {config['time_field']}")
        print(f"  主键: {config['primary_key']}")
        print(f"  排序字段: {', '.join(config['order_fields'])}")
        print()

def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="时间序列数据获取工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 流式获取推文数据
  python cli.py stream tweets --start-time "2024-01-01" --end-time "2024-01-02"
  
  # 导出用户数据到JSON文件
  python cli.py export users --start-time "2024-01-01 00:00:00" --end-time "2024-01-01 23:59:59" -o users.json
  
  # 统计回复数据数量
  python cli.py count replies --start-time "2024-01-01" --end-time "2024-01-02"
  
  # 列出所有支持的表
  python cli.py list-tables
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # 流式数据命令
    stream_parser = subparsers.add_parser("stream", help="流式获取数据")
    stream_parser.add_argument("table", choices=list(TABLE_CONFIGS.keys()), help="表名")
    stream_parser.add_argument("--start-time", required=True, help="开始时间 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)")
    stream_parser.add_argument("--end-time", required=True, help="结束时间 (YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)")
    stream_parser.add_argument("--format", choices=SUPPORTED_FORMATS, default="json", help="输出格式")
    stream_parser.add_argument("--chunk-size", type=int, default=1000, help="分块大小")
    stream_parser.add_argument("--time-interval", type=int, default=60, help="时间窗口间隔(分钟)")
    stream_parser.add_argument("--max-connections", type=int, default=3, help="最大数据库连接数")
    stream_parser.add_argument("--use-time-windows", action="store_true", default=True, help="使用时间窗口分块")
    stream_parser.add_argument("--no-time-windows", dest="use_time_windows", action="store_false", help="不使用时间窗口分块")
    stream_parser.add_argument("--where", help="额外的WHERE条件")
    stream_parser.add_argument("--fields", nargs="+", help="要选择的字段列表")
    stream_parser.add_argument("--output", help="输出文件路径(不指定则输出到控制台)")
    stream_parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    
    # 导出数据命令
    export_parser = subparsers.add_parser("export", help="导出数据到文件")
    export_parser.add_argument("table", choices=list(TABLE_CONFIGS.keys()), help="表名")
    export_parser.add_argument("--start-time", required=True, help="开始时间")
    export_parser.add_argument("--end-time", required=True, help="结束时间")
    export_parser.add_argument("-o", "--output", required=True, help="输出文件路径")
    export_parser.add_argument("--format", choices=SUPPORTED_FORMATS, default="json", help="输出格式")
    export_parser.add_argument("--chunk-size", type=int, default=1000, help="分块大小")
    export_parser.add_argument("--time-interval", type=int, default=60, help="时间窗口间隔(分钟)")
    export_parser.add_argument("--max-connections", type=int, default=3, help="最大数据库连接数")
    export_parser.add_argument("--use-time-windows", action="store_true", default=True, help="使用时间窗口分块")
    export_parser.add_argument("--no-time-windows", dest="use_time_windows", action="store_false", help="不使用时间窗口分块")
    export_parser.add_argument("--where", help="额外的WHERE条件")
    export_parser.add_argument("--fields", nargs="+", help="要选择的字段列表")
    
    # 统计数据命令
    count_parser = subparsers.add_parser("count", help="统计数据数量")
    count_parser.add_argument("table", choices=list(TABLE_CONFIGS.keys()), help="表名")
    count_parser.add_argument("--start-time", required=True, help="开始时间")
    count_parser.add_argument("--end-time", required=True, help="结束时间")
    count_parser.add_argument("--where", help="额外的WHERE条件")
    count_parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    
    # 列出表命令
    list_parser = subparsers.add_parser("list-tables", help="列出所有支持的表")
    
    return parser

async def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == "stream":
            await stream_data_command(args)
        elif args.command == "export":
            await export_data_command(args)
        elif args.command == "count":
            await count_data_command(args)
        elif args.command == "list-tables":
            list_tables_command(args)
        else:
            parser.print_help()
    
    except KeyboardInterrupt:
        print("\n用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
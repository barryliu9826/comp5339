#!/usr/bin/env python3
"""
JSON to CSV Converter for Energy Data
将能源数据的JSON文件转换为CSV格式

Author: Principal AI/ML Systems Engineer Agent
Date: 2025-01-27
"""

import json
import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONToCSVConverter:
    """JSON数据转换为CSV的核心类"""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "csv_output"):
        """
        初始化转换器
        
        Args:
            data_dir: JSON数据文件目录
            output_dir: CSV输出目录
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # 数据文件路径
        self.facilities_file = self.data_dir / "facilities_data.json"
        self.metrics_file = self.data_dir / "facility_metrics.json"
        self.market_file = self.data_dir / "market_data.json"
        
    def load_json_data(self, file_path: Path) -> Dict[str, Any]:
        """
        加载JSON数据文件
        
        Args:
            file_path: JSON文件路径
            
        Returns:
            解析后的JSON数据
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"成功加载文件: {file_path}")
            return data
        except Exception as e:
            logger.error(f"加载文件失败 {file_path}: {e}")
            raise
    
    def convert_facilities_data(self) -> None:
        """转换设施数据为CSV格式"""
        logger.info("开始转换设施数据...")
        
        # 加载数据
        data = self.load_json_data(self.facilities_file)
        facilities_data = data.get('data', [])
        
        # 准备设施主表数据
        facilities_records = []
        units_records = []
        
        for facility in facilities_data:
            # 提取设施基本信息
            facility_record = {
                'code': facility.get('code'),
                'name': facility.get('name'),
                'network_id': facility.get('network_id'),
                'network_region': facility.get('network_region'),
                'description': facility.get('description', ''),
                'lat': facility.get('location', {}).get('lat'),
                'lng': facility.get('location', {}).get('lng'),
                'created_at': facility.get('created_at'),
                'updated_at': facility.get('updated_at')
            }
            facilities_records.append(facility_record)
            
            # 提取单元信息
            units = facility.get('units', [])
            for unit in units:
                unit_record = {
                    'facility_code': facility.get('code'),
                    'unit_code': unit.get('code'),
                    'fueltech_id': unit.get('fueltech_id'),
                    'status_id': unit.get('status_id'),
                    'capacity_registered': unit.get('capacity_registered'),
                    'capacity_maximum': unit.get('capacity_maximum'),
                    'data_first_seen': unit.get('data_first_seen'),
                    'data_last_seen': unit.get('data_last_seen'),
                    'dispatch_type': unit.get('dispatch_type'),
                    'emissions_factor_co2': unit.get('emissions_factor_co2'),
                    'created_at': unit.get('created_at'),
                    'updated_at': unit.get('updated_at')
                }
                units_records.append(unit_record)
        
        # 创建DataFrame并保存为CSV
        facilities_df = pd.DataFrame(facilities_records)
        units_df = pd.DataFrame(units_records)
        
        # 保存CSV文件
        facilities_csv_path = self.output_dir / "facilities.csv"
        units_csv_path = self.output_dir / "facility_units.csv"
        
        facilities_df.to_csv(facilities_csv_path, index=False, encoding='utf-8')
        units_df.to_csv(units_csv_path, index=False, encoding='utf-8')
        
        logger.info(f"设施数据转换完成: {len(facilities_records)} 个设施, {len(units_records)} 个单元")
        logger.info(f"输出文件: {facilities_csv_path}, {units_csv_path}")
    
    def convert_facility_metrics_data(self) -> None:
        """转换设施指标数据为CSV格式"""
        logger.info("开始转换设施指标数据...")
        
        # 加载数据
        data = self.load_json_data(self.metrics_file)
        metrics_data = data.get('data', [])
        
        # 准备元数据记录
        metadata_records = []
        data_records = []
        
        for metric_group in metrics_data:
            # 提取元数据
            metadata_record = {
                'network_code': metric_group.get('network_code'),
                'metric': metric_group.get('metric'),
                'unit': metric_group.get('unit'),
                'interval': metric_group.get('interval'),
                'date_start': metric_group.get('date_start'),
                'date_end': metric_group.get('date_end'),
                'network_timezone_offset': metric_group.get('network_timezone_offset')
            }
            metadata_records.append(metadata_record)
            
            # 提取时间序列数据
            results = metric_group.get('results', [])
            for result in results:
                unit_code = result.get('columns', {}).get('unit_code', 'unknown')
                data_points = result.get('data', [])
                
                for data_point in data_points:
                    if len(data_point) >= 2:
                        data_record = {
                            'network_code': metric_group.get('network_code'),
                            'metric': metric_group.get('metric'),
                            'unit_code': unit_code,
                            'timestamp': data_point[0],
                            'value': data_point[1]
                        }
                        data_records.append(data_record)
        
        # 创建DataFrame并保存为CSV
        metadata_df = pd.DataFrame(metadata_records)
        data_df = pd.DataFrame(data_records)
        
        # 保存CSV文件
        metadata_csv_path = self.output_dir / "facility_metrics_metadata.csv"
        data_csv_path = self.output_dir / "facility_metrics_data.csv"
        
        metadata_df.to_csv(metadata_csv_path, index=False, encoding='utf-8')
        data_df.to_csv(data_csv_path, index=False, encoding='utf-8')
        
        logger.info(f"设施指标数据转换完成: {len(metadata_records)} 个指标组, {len(data_records)} 个数据点")
        logger.info(f"输出文件: {metadata_csv_path}, {data_csv_path}")
    
    def convert_market_data(self) -> None:
        """转换市场数据为CSV格式"""
        logger.info("开始转换市场数据...")
        
        # 加载数据
        data = self.load_json_data(self.market_file)
        market_data = data.get('data', [])
        
        # 准备元数据记录
        metadata_records = []
        data_records = []
        
        for metric_group in market_data:
            # 提取元数据
            metadata_record = {
                'network_code': metric_group.get('network_code'),
                'metric': metric_group.get('metric'),
                'unit': metric_group.get('unit'),
                'interval': metric_group.get('interval'),
                'date_start': metric_group.get('date_start'),
                'date_end': metric_group.get('date_end'),
                'network_timezone_offset': metric_group.get('network_timezone_offset')
            }
            metadata_records.append(metadata_record)
            
            # 提取时间序列数据
            results = metric_group.get('results', [])
            for result in results:
                data_points = result.get('data', [])
                
                for data_point in data_points:
                    if len(data_point) >= 2:
                        data_record = {
                            'network_code': metric_group.get('network_code'),
                            'metric': metric_group.get('metric'),
                            'timestamp': data_point[0],
                            'value': data_point[1]
                        }
                        data_records.append(data_record)
        
        # 创建DataFrame并保存为CSV
        metadata_df = pd.DataFrame(metadata_records)
        data_df = pd.DataFrame(data_records)
        
        # 保存CSV文件
        metadata_csv_path = self.output_dir / "market_metrics_metadata.csv"
        data_csv_path = self.output_dir / "market_metrics_data.csv"
        
        metadata_df.to_csv(metadata_csv_path, index=False, encoding='utf-8')
        data_df.to_csv(data_csv_path, index=False, encoding='utf-8')
        
        logger.info(f"市场数据转换完成: {len(metadata_records)} 个指标组, {len(data_records)} 个数据点")
        logger.info(f"输出文件: {metadata_csv_path}, {data_csv_path}")
    
    def validate_data_integrity(self) -> None:
        """验证数据完整性"""
        logger.info("开始数据完整性验证...")
        
        # 检查输出文件是否存在
        expected_files = [
            "facilities.csv",
            "facility_units.csv", 
            "facility_metrics_metadata.csv",
            "facility_metrics_data.csv",
            "market_metrics_metadata.csv",
            "market_metrics_data.csv"
        ]
        
        for filename in expected_files:
            file_path = self.output_dir / filename
            if file_path.exists():
                df = pd.read_csv(file_path)
                logger.info(f"✓ {filename}: {len(df)} 行数据")
            else:
                logger.warning(f"✗ {filename}: 文件不存在")
        
        # 验证外键关系
        try:
            facilities_df = pd.read_csv(self.output_dir / "facilities.csv")
            units_df = pd.read_csv(self.output_dir / "facility_units.csv")
            
            # 检查设施代码引用完整性
            facility_codes = set(facilities_df['code'])
            unit_facility_codes = set(units_df['facility_code'])
            
            missing_facilities = unit_facility_codes - facility_codes
            if missing_facilities:
                logger.warning(f"发现孤立单元引用: {missing_facilities}")
            else:
                logger.info("✓ 设施-单元关系完整性验证通过")
                
        except Exception as e:
            logger.error(f"数据完整性验证失败: {e}")
    
    def generate_summary_report(self) -> None:
        """生成转换摘要报告"""
        logger.info("生成转换摘要报告...")
        
        report_path = self.output_dir / "conversion_report.txt"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("JSON to CSV 转换摘要报告\n")
            f.write("=" * 50 + "\n")
            f.write(f"转换时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 统计各文件数据量
            csv_files = list(self.output_dir.glob("*.csv"))
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    f.write(f"{csv_file.name}: {len(df)} 行, {len(df.columns)} 列\n")
                except Exception as e:
                    f.write(f"{csv_file.name}: 读取失败 - {e}\n")
        
        logger.info(f"摘要报告已生成: {report_path}")
    
    def run_conversion(self) -> None:
        """执行完整的转换流程"""
        logger.info("开始JSON到CSV转换流程...")
        
        try:
            # 转换各类数据
            self.convert_facilities_data()
            self.convert_facility_metrics_data()
            self.convert_market_data()
            
            # 验证数据完整性
            self.validate_data_integrity()
            
            # 生成摘要报告
            self.generate_summary_report()
            
            logger.info("转换流程完成!")
            
        except Exception as e:
            logger.error(f"转换流程失败: {e}")
            raise


def main():
    """主函数"""
    print("JSON to CSV Converter for Energy Data")
    print("=" * 50)
    
    # 创建转换器实例
    converter = JSONToCSVConverter()
    
    # 执行转换
    converter.run_conversion()
    
    print("\n转换完成! 输出文件位于 'csv_output' 目录")
    print("请查看 'conversion.log' 文件了解详细日志")


if __name__ == "__main__":
    main()

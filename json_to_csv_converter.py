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
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONToCSVConverter:
    """JSON数据转换为CSV的核心类"""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "data/csv_output"):
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
        """
        转换设施数据为CSV格式
        
        将嵌套的JSON结构转换为两个关系化的CSV文件:
        - facilities.csv: 设施主表（扁平化结构）
        - facility_units.csv: 设施单元子表（包含所有单元字段）
        
        Raises:
            FileNotFoundError: JSON文件不存在
            KeyError: 必需字段缺失
            json.JSONDecodeError: JSON解析失败
        """
        logger.info("开始转换设施数据...")
        
        # 加载数据
        data = self.load_json_data(self.facilities_file)
        
        # 验证顶层结构
        if not isinstance(data, dict):
            error_msg = f"无效的JSON结构: 期望dict类型，实际{type(data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "JSON文件中缺少'data'字段"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        facilities_data = data.get('data', [])
        
        if not isinstance(facilities_data, list):
            error_msg = f"'data'字段必须是list类型，实际{type(facilities_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 准备设施主表数据
        facilities_records: List[Dict[str, Any]] = []
        units_records: List[Dict[str, Any]] = []
        
        for facility in facilities_data:
            # 验证设施必需字段
            if 'code' not in facility:
                error_msg = f"设施记录缺少必需字段'code': {facility}"
                logger.error(error_msg)
                raise KeyError(error_msg)
            
            # 提取设施基本信息（扁平化location对象）
            location = facility.get('location', {})
            if not isinstance(location, dict):
                location = {}
            
            facility_record = {
                'code': facility.get('code'),
                'name': facility.get('name'),
                'network_id': facility.get('network_id'),
                'network_region': facility.get('network_region'),
                'description': facility.get('description', ''),
                'npi_id': facility.get('npi_id'),
                'lat': location.get('lat') if isinstance(location, dict) else None,
                'lng': location.get('lng') if isinstance(location, dict) else None,
                'created_at': facility.get('created_at'),
                'updated_at': facility.get('updated_at')
            }
            facilities_records.append(facility_record)
            
            # 提取单元信息（包含所有字段）
            units = facility.get('units', [])
            if not isinstance(units, list):
                units = []
            
            facility_code = facility.get('code')
            
            for unit in units:
                if not isinstance(unit, dict):
                    logger.warning(f"跳过无效单元记录 (设施: {facility_code}): {type(unit).__name__}")
                    continue
                
                # 提取所有单元字段（包括所有日期相关字段）
                unit_record = {
                    'facility_code': facility_code,
                    'unit_code': unit.get('code'),
                    'fueltech_id': unit.get('fueltech_id'),
                    'status_id': unit.get('status_id'),
                    'capacity_registered': unit.get('capacity_registered'),
                    'capacity_maximum': unit.get('capacity_maximum'),
                    'capacity_storage': unit.get('capacity_storage'),
                    'emissions_factor_co2': unit.get('emissions_factor_co2'),
                    'data_first_seen': unit.get('data_first_seen'),
                    'data_last_seen': unit.get('data_last_seen'),
                    'dispatch_type': unit.get('dispatch_type'),
                    # 开始日期相关字段
                    'commencement_date': unit.get('commencement_date'),
                    'commencement_date_specificity': unit.get('commencement_date_specificity'),
                    'commencement_date_display': unit.get('commencement_date_display'),
                    # 关闭日期相关字段
                    'closure_date': unit.get('closure_date'),
                    'closure_date_specificity': unit.get('closure_date_specificity'),
                    'closure_date_display': unit.get('closure_date_display'),
                    # 预期运营日期相关字段
                    'expected_operation_date': unit.get('expected_operation_date'),
                    'expected_operation_date_specificity': unit.get('expected_operation_date_specificity'),
                    'expected_operation_date_display': unit.get('expected_operation_date_display'),
                    # 预期关闭日期相关字段
                    'expected_closure_date': unit.get('expected_closure_date'),
                    'expected_closure_date_specificity': unit.get('expected_closure_date_specificity'),
                    'expected_closure_date_display': unit.get('expected_closure_date_display'),
                    # 建设开始日期相关字段
                    'construction_start_date': unit.get('construction_start_date'),
                    'construction_start_date_specificity': unit.get('construction_start_date_specificity'),
                    'construction_start_date_display': unit.get('construction_start_date_display'),
                    # 项目批准日期相关字段
                    'project_approval_date': unit.get('project_approval_date'),
                    'project_approval_date_specificity': unit.get('project_approval_date_specificity'),
                    'project_approval_date_display': unit.get('project_approval_date_display'),
                    # 项目提交日期
                    'project_lodgement_date': unit.get('project_lodgement_date'),
                    # 元数据时间戳
                    'created_at': unit.get('created_at'),
                    'updated_at': unit.get('updated_at')
                }
                units_records.append(unit_record)
        
        # 创建DataFrame并保存为CSV
        facilities_df = pd.DataFrame(facilities_records)
        units_df = pd.DataFrame(units_records)
        
        # 保存CSV文件（使用NA表示空值）
        facilities_csv_path = self.output_dir / "facilities.csv"
        units_csv_path = self.output_dir / "facility_units.csv"
        
        facilities_df.to_csv(facilities_csv_path, index=False, encoding='utf-8', na_rep='NA')
        units_df.to_csv(units_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        logger.info(f"设施数据转换完成: {len(facilities_records)} 个设施, {len(units_records)} 个单元")
        logger.info(f"输出文件: {facilities_csv_path}, {units_csv_path}")
        logger.info(f"设施表列数: {len(facilities_df.columns)}, 单元表列数: {len(units_df.columns)}")
    
    def convert_facility_metrics_data(self) -> None:
        """
        转换设施指标数据为宽格式CSV
        
        目标结构：每行包含 facility_code, timestamp, power, emissions, longitude, latitude
        
        处理流程:
        1. 从 JSON 加载数据并转换为长格式 DataFrame
        2. 使用 pivot_table 将长格式转换为宽格式（power 和 emissions 作为列）
        3. 关联 facilities 数据获取地理位置（longitude, latitude）
        4. 验证数据完整性
        5. 保存为宽格式 CSV
        
        Raises:
            ValueError: 数据格式不正确
            KeyError: 必需字段缺失
            FileNotFoundError: facilities CSV 文件不存在
        """
        logger.info("开始转换设施指标数据（宽格式）...")
        
        # 加载 JSON 数据
        data = self.load_json_data(self.metrics_file)
        
        # 验证顶层结构
        if not isinstance(data, dict):
            error_msg = f"无效的JSON结构: 期望dict类型，实际{type(data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        if 'data' not in data:
            error_msg = "JSON文件中缺少'data'字段"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        metrics_data = data.get('data', {})
        
        if not isinstance(metrics_data, dict):
            error_msg = f"'data'字段必须是dict类型，实际{type(metrics_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 准备数据记录（长格式）
        data_records: List[Dict[str, Any]] = []
        
        # 遍历每个指标类型（power, emissions等）
        for metric_name, metric_data in metrics_data.items():
            if not isinstance(metric_data, dict):
                logger.warning(f"跳过无效指标数据: {metric_name} (类型: {type(metric_data).__name__})")
                continue
            
            # 提取设施数据
            facilities = metric_data.get('facilities', {})
            
            if not isinstance(facilities, dict):
                logger.warning(f"跳过无效设施数据: {metric_name} (设施数据类型: {type(facilities).__name__})")
                continue
            
            # 遍历每个设施的时间序列数据
            for facility_code, time_series in facilities.items():
                if not isinstance(time_series, dict):
                    logger.warning(f"跳过无效时间序列: {metric_name}/{facility_code}")
                    continue
                
                # 遍历时间戳和值
                for timestamp, value in time_series.items():
                    data_record = {
                        'facility_code': facility_code,
                        'timestamp': timestamp,
                        'metric': metric_name,
                        'value': value
                    }
                    data_records.append(data_record)
        
        if not data_records:
            error_msg = "未找到有效的指标数据"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 创建长格式 DataFrame
        long_df = pd.DataFrame(data_records)
        
        logger.info(f"长格式数据: {len(long_df)} 行")
        logger.info(f"唯一设施数: {long_df['facility_code'].nunique()}")
        logger.info(f"唯一时间戳数: {long_df['timestamp'].nunique()}")
        
        # 步骤1: 数据透视（Long to Wide）
        # 将 metric 列作为列名，value 作为值
        wide_df = long_df.pivot_table(
            index=['facility_code', 'timestamp'],
            columns='metric',
            values='value',
            aggfunc='first',  # 如果有重复值，取第一个
            fill_value=None  # 缺失值使用 None（后续会转换为 NA）
        )
        
        # 重置索引，将 facility_code 和 timestamp 转换为列
        wide_df = wide_df.reset_index()
        
        # 移除列索引名称（如果有）
        wide_df.columns.name = None
        
        logger.info(f"宽格式数据: {len(wide_df)} 行")
        logger.info(f"宽格式列: {list(wide_df.columns)}")
        
        # 步骤2: 关联 facilities 数据获取地理位置
        facilities_csv_path = self.output_dir / "facilities.csv"
        
        if not facilities_csv_path.exists():
            error_msg = f"设施数据文件不存在: {facilities_csv_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # 读取 facilities 数据
        facilities_df = pd.read_csv(facilities_csv_path)
        
        # 验证必需列存在
        required_cols = ['code', 'lat', 'lng']
        missing_cols = [col for col in required_cols if col not in facilities_df.columns]
        if missing_cols:
            error_msg = f"设施数据缺少必需列: {missing_cols}"
            logger.error(error_msg)
            raise KeyError(error_msg)
        
        # 左连接：保留所有 metrics 记录，关联地理位置
        wide_df = wide_df.merge(
            facilities_df[['code', 'lat', 'lng']],
            left_on='facility_code',
            right_on='code',
            how='left'
        )
        
        # 步骤3: 列重命名和排序
        # 重命名 lat → latitude, lng → longitude
        wide_df = wide_df.rename(columns={'lat': 'latitude', 'lng': 'longitude'})
        
        # 删除 code 列（已关联，不再需要）
        if 'code' in wide_df.columns:
            wide_df = wide_df.drop(columns=['code'])
        
        # 确保列顺序：facility_code, timestamp, power, emissions, longitude, latitude
        # 如果某些指标不存在，也会包含在最终列中
        column_order = ['facility_code', 'timestamp']
        
        # 添加指标列（按指定顺序：power, emissions）
        metric_order = ['power', 'emissions']
        metric_cols = [col for col in metric_order if col in wide_df.columns]
        column_order.extend(metric_cols)
        
        # 添加地理位置列
        column_order.extend(['longitude', 'latitude'])
        
        # 确保所有列都存在，如果不存在则创建为 NA
        for col in column_order:
            if col not in wide_df.columns:
                wide_df[col] = None
        
        # 重新排序列
        wide_df = wide_df[column_order]
        
        # 按 facility_code 和 timestamp 排序
        wide_df = wide_df.sort_values(['facility_code', 'timestamp'])
        
        # 步骤4: 数据验证
        expected_rows = wide_df['facility_code'].nunique() * wide_df['timestamp'].nunique()
        actual_rows = len(wide_df)
        
        if actual_rows != expected_rows:
            logger.warning(f"行数不匹配: 期望 {expected_rows}, 实际 {actual_rows}")
        
        # 检查缺失值
        missing_power = wide_df['power'].isna().sum()
        missing_emissions = wide_df['emissions'].isna().sum()
        missing_longitude = wide_df['longitude'].isna().sum()
        missing_latitude = wide_df['latitude'].isna().sum()
        
        logger.info(f"数据完整性检查:")
        logger.info(f"  - 总行数: {actual_rows}")
        logger.info(f"  - 缺失 power 值: {missing_power} ({missing_power/actual_rows*100:.2f}%)")
        logger.info(f"  - 缺失 emissions 值: {missing_emissions} ({missing_emissions/actual_rows*100:.2f}%)")
        logger.info(f"  - 缺失 longitude 值: {missing_longitude} ({missing_longitude/actual_rows*100:.2f}%)")
        logger.info(f"  - 缺失 latitude 值: {missing_latitude} ({missing_latitude/actual_rows*100:.2f}%)")
        
        # 检查是否有设施代码无法匹配
        unmatched_facilities = wide_df[wide_df['longitude'].isna()]['facility_code'].unique()
        if len(unmatched_facilities) > 0:
            logger.warning(f"无法匹配地理位置的设施数: {len(unmatched_facilities)}")
            logger.warning(f"前10个未匹配设施: {list(unmatched_facilities[:10])}")
        
        # 步骤5: 保存为宽格式 CSV
        wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
        
        wide_df.to_csv(wide_csv_path, index=False, encoding='utf-8', na_rep='NA')
        
        logger.info(f"设施指标数据转换完成（宽格式）: {len(wide_df)} 行, {len(wide_df.columns)} 列")
        logger.info(f"输出文件: {wide_csv_path}")
        logger.info(f"列顺序: {', '.join(wide_df.columns)}")
    
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
            "facility_metrics_wide.csv",
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
            
            # 验证 facility_metrics_wide 中的 facility_code 是否在 facilities 中
            wide_csv_path = self.output_dir / "facility_metrics_wide.csv"
            if wide_csv_path.exists():
                wide_df = pd.read_csv(wide_csv_path)
                wide_facility_codes = set(wide_df['facility_code'].unique())
                missing_in_wide = wide_facility_codes - facility_codes
                if missing_in_wide:
                    logger.warning(f"facility_metrics_wide 中发现未匹配的设施代码: {len(missing_in_wide)} 个")
                else:
                    logger.info("✓ facility_metrics_wide 设施代码完整性验证通过")
                
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


def main_facilities_only() -> None:
    """
    仅转换 facilities 数据的主函数
    
    执行 facilities_data.json 到 CSV 的转换，输出:
        - csv_output/facilities.csv: 设施主表
        - csv_output/facility_units.csv: 设施单元子表
    """
    print("Facilities JSON to CSV Converter")
    print("=" * 50)
    
    try:
        # 创建转换器实例（只转换 facilities 数据）
        converter = JSONToCSVConverter(data_dir="data", output_dir="data/csv_output")
        
        # 仅执行 facilities 数据转换
        converter.convert_facilities_data()
        
        # 验证数据完整性并显示统计信息
        try:
            # 读取原始JSON数据以获取输入统计
            facilities_json_data = converter.load_json_data(converter.facilities_file)
            facilities_count = len(facilities_json_data.get('data', []))
            
            # 读取生成的CSV文件以获取输出统计
            output_facilities_df = pd.read_csv(converter.output_dir / "facilities.csv")
            output_units_df = pd.read_csv(converter.output_dir / "facility_units.csv")
            
            print(f"\n转换结果:")
            print(f"  - 输入设施数: {facilities_count}")
            print(f"  - 输出设施记录: {len(output_facilities_df)} 行")
            print(f"  - 输出单元记录: {len(output_units_df)} 行")
            print(f"  - 设施表列数: {len(output_facilities_df.columns)}")
            print(f"  - 单元表列数: {len(output_units_df.columns)}")
            print(f"\n输出文件:")
            print(f"  - {converter.output_dir / 'facilities.csv'}")
            print(f"  - {converter.output_dir / 'facility_units.csv'}")
            
        except Exception as e:
            logger.warning(f"数据验证跳过: {e}")
        
        print("\n✓ 转换完成!")
        print(f"请查看 'conversion.log' 文件了解详细日志")
        
    except FileNotFoundError as e:
        logger.error(f"文件未找到: {e}")
        print(f"\n✗ 错误: 文件未找到")
        raise
    except KeyError as e:
        logger.error(f"数据格式错误: {e}")
        print(f"\n✗ 错误: 数据格式错误 - {e}")
        raise
    except Exception as e:
        logger.error(f"转换失败: {e}")
        print(f"\n✗ 错误: 转换失败 - {e}")
        raise


def main() -> None:
    """
    主函数：支持命令行参数选择转换模式
    
    用法:
        python json_to_csv_converter.py              # 转换所有数据（facility_metrics 为宽格式）
        python json_to_csv_converter.py --facilities # 仅转换 facilities 数据
    """
    parser = argparse.ArgumentParser(
        description="JSON to CSV Converter for Energy Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python json_to_csv_converter.py                    # 转换所有数据（facility_metrics 为宽格式）
  python json_to_csv_converter.py --facilities       # 仅转换 facilities 数据
        """
    )
    
    parser.add_argument(
        '--facilities',
        action='store_true',
        help='仅转换 facilities_data.json 为 CSV 格式'
    )
    
    args = parser.parse_args()
    
    # 根据参数选择转换模式
    if args.facilities:
        main_facilities_only()
    else:
        print("JSON to CSV Converter for Energy Data")
        print("=" * 50)
        
        # 创建转换器实例
        converter = JSONToCSVConverter()
        
        # 确保 facilities.csv 存在（宽格式转换需要）
        facilities_csv_path = converter.output_dir / "facilities.csv"
        if not facilities_csv_path.exists():
            logger.info("Facilities CSV 不存在，先转换 facilities 数据...")
            converter.convert_facilities_data()
        
        # 执行转换（facility_metrics 默认使用宽格式）
        converter.run_conversion()
        
        print("\n转换完成! 输出文件位于 'data/csv_output' 目录")
        print("请查看 'conversion.log' 文件了解详细日志")


if __name__ == "__main__":
    main()

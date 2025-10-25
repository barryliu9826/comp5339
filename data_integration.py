#!/usr/bin/env python3
"""
数据集成和物化模块
功能：整合发电量和CO2排放数据，输出CSV文件
作者：Principal AI/ML Systems Engineer Agent
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
import os
from pathlib import Path

# 确保日志目录存在
os.makedirs('logs', exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/data_integration.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class DataIntegrationProcessor:
    """数据集成处理器"""
    
    def __init__(self, input_file: str = "data/facility_metrics.json", 
                 output_file: str = "data/consolidated_facility_data.csv"):
        """
        初始化数据集成处理器
        
        Args:
            input_file: 输入JSON文件路径
            output_file: 输出CSV文件路径
        """
        self.input_file = input_file
        self.output_file = output_file
        self.processed_records = 0
        self.error_records = 0
        
    def load_json_data(self) -> Dict[str, Any]:
        """
        加载JSON数据文件
        
        Returns:
            解析后的JSON数据
            
        Raises:
            FileNotFoundError: 文件不存在
            json.JSONDecodeError: JSON格式错误
        """
        logger.info(f"开始加载数据文件: {self.input_file}")
        
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"数据文件不存在: {self.input_file}")
            
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"成功加载数据文件，包含 {len(data.get('data', []))} 个数据批次")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误: {e}")
            raise
        except Exception as e:
            logger.error(f"文件读取错误: {e}")
            raise
    
    def extract_facility_data(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从JSON数据中提取设施数据
        
        Args:
            json_data: 原始JSON数据
            
        Returns:
            提取的设施数据列表
        """
        logger.info("开始提取设施数据")
        facility_data = []
        
        for batch in json_data.get('data', []):
            metric = batch.get('metric', '')
            network_code = batch.get('network_code', '')
            
            # 只处理NEM网络的数据
            if network_code != 'NEM':
                continue
                
            for result in batch.get('results', []):
                facility_name = result.get('name', '')
                date_start = result.get('date_start', '')
                date_end = result.get('date_end', '')
                
                # 提取时间序列数据 - 数据是数组格式 [timestamp, value]
                data_points = result.get('data', [])
                
                for point in data_points:
                    if not isinstance(point, list) or len(point) < 2:
                        continue
                        
                    timestamp = point[0]  # 时间戳
                    value = point[1]      # 数值
                    
                    # 数据清洗：移除无效值
                    if value is None or (isinstance(value, (int, float)) and np.isnan(value)):
                        continue
                        
                    # 单位转换：将吨转换为千克
                    converted_value = float(value) if value is not None else 0.0
                    if metric == 'emissions' and batch.get('unit') == 't':
                        converted_value = converted_value  # 吨转千克
                    
                    # 统一设施ID格式，移除指标前缀
                    base_facility_id = facility_name.replace('power_', '').replace('emissions_', '')
                    
                    facility_record = {
                        'facility_id': base_facility_id,
                        'timestamp': timestamp,
                        'metric_type': metric,
                        'value': converted_value,
                        'unit': batch.get('unit', ''),
                        'date_start': date_start,
                        'date_end': date_end
                    }
                    facility_data.append(facility_record)
        
        logger.info(f"成功提取 {len(facility_data)} 条设施数据记录")
        return facility_data
    
    def clean_and_validate_data(self, facility_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        清洗和验证数据
        
        Args:
            facility_data: 原始设施数据
            
        Returns:
            清洗后的数据
        """
        logger.info("开始数据清洗和验证")
        cleaned_data = []
        
        for record in facility_data:
            # 验证必需字段
            if not all(key in record for key in ['facility_id', 'timestamp', 'metric_type', 'value']):
                self.error_records += 1
                continue
                
            # 验证数值范围
            value = record.get('value', 0.0)
            if not isinstance(value, (int, float)) or np.isnan(value) or np.isinf(value):
                self.error_records += 1
                continue
                
            # 验证时间戳格式
            try:
                datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                self.error_records += 1
                continue
                
            cleaned_data.append(record)
            self.processed_records += 1
        
        logger.info(f"数据清洗完成，处理记录: {self.processed_records}, 错误记录: {self.error_records}")
        return cleaned_data
    
    def merge_facility_data(self, cleaned_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        合并设施数据，按设施和时间戳分组
        
        Args:
            cleaned_data: 清洗后的数据
            
        Returns:
            合并后的DataFrame
        """
        logger.info("开始合并设施数据")
        
        # 转换为DataFrame
        df = pd.DataFrame(cleaned_data)
        
        if df.empty:
            logger.warning("没有有效数据可以合并")
            return pd.DataFrame()
        
        # 按设施和时间戳分组，分离不同指标
        power_data = df[df['metric_type'] == 'power'].copy()
        co2_data = df[df['metric_type'] == 'emissions'].copy()
        
        # 重命名列以便合并
        power_data = power_data.rename(columns={'value': 'power_generated_mw'})
        co2_data = co2_data.rename(columns={'value': 'co2_emissions_t'})
        
        # 合并发电量和排放数据
        merged_df = pd.merge(
            power_data[['facility_id', 'timestamp', 'power_generated_mw']],
            co2_data[['facility_id', 'timestamp', 'co2_emissions_t']],
            on=['facility_id', 'timestamp'],
            how='left'  # 左连接，保留所有发电量数据
        )
        
        # 填充缺失的CO2数据
        merged_df['co2_emissions_t'] = merged_df['co2_emissions_t'].fillna(0.0)
        
        # 排序
        merged_df = merged_df.sort_values(['facility_id', 'timestamp'])
        
        logger.info(f"数据合并完成，最终记录数: {len(merged_df)}")
        return merged_df
    
    def export_to_csv(self, df: pd.DataFrame) -> None:
        """
        导出数据到CSV文件
        
        Args:
            df: 要导出的DataFrame
        """
        logger.info(f"开始导出数据到CSV文件: {self.output_file}")
        
        if df.empty:
            logger.warning("没有数据可以导出")
            return
        
        # 确保输出目录存在
        output_dir = os.path.dirname(self.output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 导出CSV
        df.to_csv(self.output_file, index=False, encoding='utf-8')
        
        logger.info(f"成功导出 {len(df)} 条记录到 {self.output_file}")
    
    
    def process(self) -> None:
        """
        执行完整的数据处理流程
        """
        logger.info("开始数据集成和物化处理")
        start_time = datetime.now()
        
        try:
            # 1. 加载数据
            json_data = self.load_json_data()
            
            # 2. 提取设施数据
            facility_data = self.extract_facility_data(json_data)
            
            # 3. 清洗和验证数据
            cleaned_data = self.clean_and_validate_data(facility_data)
            
            # 4. 合并数据
            merged_df = self.merge_facility_data(cleaned_data)
            
            # 5. 导出CSV
            self.export_to_csv(merged_df)
            
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            logger.info(f"数据处理完成，耗时: {processing_time:.2f}秒")
            logger.info(f"处理记录: {self.processed_records}, 错误记录: {self.error_records}")
            
        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            raise


def main():
    """主函数"""
    try:
        # 创建处理器实例
        processor = DataIntegrationProcessor()
        
        # 执行处理
        processor.process()
        
        print("数据集成和物化处理完成！")
        print(f"输出文件: {processor.output_file}")
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

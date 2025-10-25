#!/usr/bin/env python3
"""
OpenElectricity API设施指标数据获取工具
使用OpenElectricity SDK获取NEM网络设施指标数据（power、emissions）并保存到文件
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

from openelectricity import OEClient, DataMetric

# 日志配置
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('api_client.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def fetch_facility_metrics(facility_codes: List[str], network_code: str = "NEM", 
                          metrics: List[DataMetric] = [DataMetric.POWER, DataMetric.EMISSIONS],
                          interval: str = "5m", date_start: Optional[datetime] = None,
                          date_end: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """
    调用OpenElectricity API获取设施指标数据
    
    Args:
        facility_codes: 设施代码列表，如["ADP", "BANKSIA"]
        network_code: 网络代码，默认为"NEM"
        metrics: 指标列表，默认为[DataMetric.POWER, DataMetric.EMISSIONS]
        interval: 数据间隔，默认为"5m"
        date_start: 开始日期，默认为None
        date_end: 结束日期，默认为None
    
    Returns:
        Dict[str, Any]: API响应数据，失败时返回None
    """
    try:
        logger.info(f"开始获取设施 {facility_codes} 的指标数据...")
        logger.info(f"网络代码: {network_code}, 指标: {metrics}")
        logger.info(f"数据间隔: {interval}")
        if date_start:
            logger.info(f"开始日期: {date_start.strftime('%Y-%m-%d %H:%M:%S')}")
        if date_end:
            logger.info(f"结束日期: {date_end.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 使用OEClient获取设施指标数据
        with OEClient() as client:
            logger.info("正在连接OpenElectricity API...")
            
            # 调用get_facility_data获取指标数据
            logger.info("正在获取设施指标数据...")
            response = client.get_facility_data(
                network_code=network_code,
                facility_code=facility_codes,  # 尝试单数形式
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
            
            logger.info("API调用成功")
            logger.info(f"成功获取数据，记录总数: {len(response.data) if hasattr(response, 'data') else '未知'}")
            
            # 直接返回API响应数据，不进行任何解析
            logger.info("API调用成功，直接返回原始数据")
            return response
        
    except Exception as e:
        logger.error(f"获取设施指标数据失败: {e}")
        return None


def process_metrics_response(response_data) -> Optional[Dict[str, Any]]:
    """
    处理API响应数据，转换为可序列化的字典格式
    
    Args:
        response_data: API响应对象
        
    Returns:
        Dict[str, Any]: 处理后的数据，失败时返回None
    """
    try:
        # 将API响应对象转换为字典格式
        if hasattr(response_data, '__dict__'):
            # 如果是对象，转换为字典
            data = response_data.__dict__.copy()
        elif hasattr(response_data, 'data'):
            # 如果有data属性，直接使用
            data = {
                "data": response_data.data,
                "created_at": datetime.now().isoformat()
            }
        else:
            # 其他情况，直接使用原始数据
            data = response_data
        
        # 处理datetime对象的序列化问题
        def convert_datetime(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif hasattr(obj, '__dict__'):
                return {k: convert_datetime(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, list):
                return [convert_datetime(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: convert_datetime(v) for k, v in obj.items()}
            else:
                return obj
        
        # 递归转换所有datetime对象
        data = convert_datetime(data)
        
        logger.info("API响应数据处理完成")
        return data
        
    except Exception as e:
        logger.error(f"指标数据处理失败: {e}")
        return None


def save_metrics_to_file(data: Dict[str, Any], filename: str = 'facility_metrics.json') -> bool:
    """
    将指标数据保存为JSON文件
    
    Args:
        data: 要保存的数据
        filename: 文件名
        
    Returns:
        bool: 保存成功返回True，失败返回False
    """
    try:
        # 确保数据目录存在
        import os
        os.makedirs('data', exist_ok=True)
        
        filepath = f'data/{filename}'
        
        # 自定义JSON序列化器，处理datetime对象
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif hasattr(obj, '__dict__'):
                return obj.__dict__
            else:
                return str(obj)
        
        # 写入JSON文件，使用自定义序列化器
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)
        
        logger.info(f"指标数据已成功保存到文件: {filepath}")
        
        # 输出文件信息
        file_size = os.path.getsize(filepath)
        logger.info(f"文件大小: {file_size} 字节")
        
        return True
        
    except Exception as e:
        logger.error(f"指标数据文件保存失败: {e}")
        return False


def main():
    """
    主控制函数，协调整个指标数据获取流程
    """
    logger.info("=" * 50)
    logger.info("OpenElectricity API设施指标数据获取工具启动")
    logger.info("=" * 50)
    
    # 配置参数
    facility_codes = ["BAYSW1", "ERARING"]  # 使用示例中的设施代码
    network_code = "NEM"
    metrics = [DataMetric.POWER, DataMetric.EMISSIONS]
    interval = "5m"  # 使用示例中的间隔
    date_start = datetime(2024, 1, 1)  # 使用示例中的日期
    date_end = datetime(2024, 1, 2)
    
    try:
        # 1. 调用API获取指标数据
        logger.info("步骤1: 调用API获取设施指标数据")
        api_data = fetch_facility_metrics(
            facility_codes=facility_codes,
            network_code=network_code,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end
        )
        
        if api_data is None:
            logger.error("API调用失败，程序退出")
            return False
        
        # 2. 处理响应数据
        logger.info("步骤2: 处理指标响应数据")
        processed_data = process_metrics_response(api_data)
        
        if processed_data is None:
            logger.error("指标数据处理失败，程序退出")
            return False
        
        # 3. 保存数据到文件
        logger.info("步骤3: 保存指标数据到文件")
        save_success = save_metrics_to_file(processed_data)
        
        if not save_success:
            logger.error("指标数据文件保存失败，程序退出")
            return False
        
        # 4. 输出结果摘要
        logger.info("=" * 50)
        logger.info("设施指标数据获取完成！")
        logger.info(f"创建时间: {processed_data.get('created_at', '未知')}")
        logger.info(f"数据已保存到文件")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

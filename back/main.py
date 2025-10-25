#!/usr/bin/env python3
"""
OpenElectricity API数据获取工具
使用OpenElectricity SDK获取NEM网络设施数据并保存到文件
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

from openelectricity import OEClient

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


def fetch_facilities_data() -> Optional[Dict[str, Any]]:
    """
    调用OpenElectricity API获取设施数据
    
    Returns:
        Dict[str, Any]: API响应数据，失败时返回None
    """
    try:
        logger.info("开始调用OpenElectricity API...")
        
        # 使用OEClient获取设施数据
        with OEClient() as client:
            logger.info("正在获取NEM网络设施数据...")
            
            # 调用facilities API
            response = client.get_facilities(network_id=["NEM"])
            
            logger.info("API调用成功")
            logger.info(f"成功获取数据，设施总数: {len(response.data)}")
            
            # 转换为字典格式
            data = {
                "version": "4.3.0",
                "created_at": datetime.now().isoformat(),
                "success": True,
                "error": None,
                "data": [],
                "total_records": len(response.data)
            }
            
            # 转换设施数据
            for facility in response.data:
                facility_data = {
                    "code": facility.code,
                    "name": facility.name,
                    "network_id": facility.network_id,
                    "network_region": facility.network_region,
                    "description": facility.description,
                    "location": {
                        "lat": facility.location.lat if facility.location else None,
                        "lng": facility.location.lng if facility.location else None
                    },
                    "units": [],
                    "updated_at": facility.updated_at.isoformat() if facility.updated_at else None,
                    "created_at": facility.created_at.isoformat() if facility.created_at else None
                }
                
                # 转换units数据
                if facility.units:
                    for unit in facility.units:
                        unit_data = {
                            "code": unit.code,
                            "fueltech_id": unit.fueltech_id,
                            "status_id": unit.status_id,
                            "capacity_registered": unit.capacity_registered,
                            "capacity_maximum": unit.capacity_maximum,
                            "data_first_seen": unit.data_first_seen.isoformat() if unit.data_first_seen else None,
                            "data_last_seen": unit.data_last_seen.isoformat() if unit.data_last_seen else None,
                            "dispatch_type": unit.dispatch_type,
                            "created_at": unit.created_at.isoformat() if unit.created_at else None,
                            "updated_at": unit.updated_at.isoformat() if unit.updated_at else None
                        }
                        
                        # 添加可选的emissions_factor_co2字段
                        if hasattr(unit, 'emissions_factor_co2') and unit.emissions_factor_co2 is not None:
                            unit_data["emissions_factor_co2"] = unit.emissions_factor_co2
                            
                        facility_data["units"].append(unit_data)
                
                data["data"].append(facility_data)
            
            return data
        
    except Exception as e:
        logger.error(f"API调用失败: {e}")
        return None


def process_api_response(response_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    处理和验证API响应数据
    
    Args:
        response_data: API响应数据
        
    Returns:
        Dict[str, Any]: 处理后的数据，失败时返回None
    """
    try:
        # 验证响应结构
        required_fields = ['version', 'created_at', 'success', 'data']
        for field in required_fields:
            if field not in response_data:
                logger.error(f"响应数据缺少必需字段: {field}")
                return None
        
        # 检查API调用是否成功
        if not response_data.get('success', False):
            logger.error(f"API返回错误: {response_data.get('error', '未知错误')}")
            return None
        
        # 验证数据字段
        data_list = response_data.get('data', [])
        if not isinstance(data_list, list):
            logger.error("data字段不是列表格式")
            return None
        
        logger.info(f"数据验证成功，包含 {len(data_list)} 条设施记录")
        
        # 添加处理时间戳
        response_data['processed_at'] = datetime.now().isoformat()
        
        return response_data
        
    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        return None


def save_data_to_file(data: Dict[str, Any], filename: str = 'facilities_data.json') -> bool:
    """
    将数据保存为JSON文件
    
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
        
        # 写入JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"数据已成功保存到文件: {filepath}")
        
        # 输出文件信息
        file_size = os.path.getsize(filepath)
        logger.info(f"文件大小: {file_size} 字节")
        
        return True
        
    except Exception as e:
        logger.error(f"文件保存失败: {e}")
        return False


def main():
    """
    主控制函数，协调整个数据获取流程
    """
    logger.info("=" * 50)
    logger.info("OpenElectricity API数据获取工具启动")
    logger.info("=" * 50)
    
    try:
        # 1. 调用API获取数据
        logger.info("步骤1: 调用API获取数据")
        api_data = fetch_facilities_data()
        
        if api_data is None:
            logger.error("API调用失败，程序退出")
            return False
        
        # 2. 处理响应数据
        logger.info("步骤2: 处理响应数据")
        processed_data = process_api_response(api_data)
        
        if processed_data is None:
            logger.error("数据处理失败，程序退出")
            return False
        
        # 3. 保存数据到文件
        logger.info("步骤3: 保存数据到文件")
        save_success = save_data_to_file(processed_data)
        
        if not save_success:
            logger.error("文件保存失败，程序退出")
            return False
        
        # 4. 输出结果摘要
        logger.info("=" * 50)
        logger.info("数据获取完成！")
        logger.info(f"API版本: {processed_data.get('version', '未知')}")
        logger.info(f"创建时间: {processed_data.get('created_at', '未知')}")
        logger.info(f"处理时间: {processed_data.get('processed_at', '未知')}")
        logger.info(f"设施总数: {processed_data.get('total_records', '未知')}")
        logger.info(f"数据记录数: {len(processed_data.get('data', []))}")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

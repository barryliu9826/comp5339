#!/usr/bin/env python3
"""
基于 OpenElectricity 官方 SDK (OEClient) 的数据采集程序
完全独立的实现，不复用 main_restapi.py 中的任何代码

功能：
- 获取网络设施数据（Network）
- 批量获取设施指标数据（Metrics，支持分片处理）
- 获取市场数据（Market）
"""

import os
import json
import time
import math
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

# SDK 导入
from openelectricity import OEClient
from openelectricity.models.facilities import FacilityResponse, Facility
from openelectricity.models.timeseries import TimeSeriesResponse
from openelectricity.types import (
    DataMetric,
    MarketMetric,
    NetworkCode,
    DataInterval,
    DataPrimaryGrouping,
    UnitStatusType,
)

# 确保日志目录存在
os.makedirs('logs', exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/sdk_client.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ConfigLoader:
    """配置管理器，从环境变量加载配置"""
    
    @staticmethod
    def load_api_key() -> str:
        """加载 API 密钥（必需）"""
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            raise ValueError("OPENELECTRICITY_API_KEY 环境变量未设置")
        return api_key
    
    @staticmethod
    def load_base_url() -> Optional[str]:
        """加载 API 基础 URL（可选）"""
        return os.getenv("OE_BASE_URL")
    
    @staticmethod
    def load_time_window() -> Tuple[datetime, datetime]:
        """加载时间窗口（可选，默认值）"""
        
        date_start = datetime(2025, 10, 1)
        date_end = datetime(2025, 10, 2)
        
        return date_start, date_end
    
    @staticmethod
    def load_interval() -> str:
        """加载数据间隔（可选，默认 '5m'）"""
        return os.getenv("INTERVAL", "5m")
    
    @staticmethod
    def load_batch_config() -> Dict[str, Any]:
        """加载批量处理配置"""
        batch_size = int(os.getenv("BATCH_SIZE", "30"))
        batch_delay = float(os.getenv("BATCH_DELAY_SECS", "0.5"))
        return {
            "batch_size": batch_size,
            "batch_delay": batch_delay
        }
    
    @staticmethod
    def load_network_filter() -> Optional[str]:
        """加载网络过滤条件（可选，默认 'NEM'）"""
        return os.getenv("NETWORK_FILTER", "NEM")
    
    @staticmethod
    def load_status_filter() -> Optional[str]:
        """加载状态过滤条件（可选，默认 'operating'）"""
        return os.getenv("STATUS_FILTER", "operating")


class DataConverter:
    """数据转换器，将 Pydantic 模型转换为字典格式"""
    
    @staticmethod
    def facility_response_to_dict(response: FacilityResponse) -> Dict[str, Any]:
        """将 FacilityResponse 转换为字典"""
        return response.model_dump()
    
    @staticmethod
    def timeseries_response_to_dict(
        response: TimeSeriesResponse,
        data_type: str = "metrics"
    ) -> Dict[str, Any]:
        """将 TimeSeriesResponse 转换为字典"""
        data = response.model_dump()
        data['data_type'] = data_type
        return data
    
    @staticmethod
    def add_metadata(
        data: Dict[str, Any],
        data_type: str
    ) -> Dict[str, Any]:
        """添加处理元数据"""
        data['processed_at'] = datetime.now().isoformat()
        if 'data_type' not in data:
            data['data_type'] = data_type
        if 'total_records' not in data:
            data_list = data.get('data', [])
            if isinstance(data_list, list):
                data['total_records'] = len(data_list)
        return data


class FacilityCodeExtractor:
    """设施代码提取器"""
    
    @staticmethod
    def extract_codes(
        facilities_response: FacilityResponse,
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[str]:
        """从设施响应中提取设施代码"""
        if not facilities_response.success:
            raise ValueError(f"设施数据获取失败: {facilities_response.error}")
        
        facilities = facilities_response.data
        if not facilities:
            logger.warning("设施数据为空")
            return []
        
        # 过滤设施
        filtered = FacilityCodeExtractor.filter_facilities(
            facilities,
            network_filter=network_filter,
            status_filter=status_filter
        )
        
        # 提取代码
        codes = []
        for facility in filtered:
            code = facility.code
            if code:
                codes.append(code)
        
        logger.info(f"成功提取 {len(codes)} 个设施代码")
        if network_filter:
            logger.info(f"应用网络过滤: {network_filter}")
        if status_filter:
            logger.info(f"应用状态过滤: {status_filter}")
        
        return codes
    
    @staticmethod
    def filter_facilities(
        facilities: List[Facility],
        network_filter: Optional[str] = None,
        status_filter: Optional[str] = None
    ) -> List[Facility]:
        """根据网络和状态过滤设施"""
        filtered = facilities
        
        # 网络过滤（network_id 是 NetworkCode Literal 类型，在 Pydantic 中已经是字符串）
        if network_filter:
            filtered = [
                f for f in filtered
                if f.network_id == network_filter
            ]
        
        # 状态过滤（检查 units 中的 status_id）
        if status_filter:
            result = []
            for facility in filtered:
                units = facility.units if facility.units else []
                # status_id 是 UnitStatusType 枚举，需要比较 value
                has_matching_status = any(
                    unit.status_id.value == status_filter
                    for unit in units
                )
                if has_matching_status:
                    result.append(facility)
            filtered = result
        
        return filtered


class BatchHandler:
    """批量处理器，处理分片和聚合"""
    
    def __init__(self, batch_size: int = 30, batch_delay: float = 2.0):
        self.batch_size = batch_size
        self.batch_delay = batch_delay
    
    def create_batches(self, items: List[str]) -> List[List[str]]:
        """创建分片"""
        batches = []
        total = len(items)
        total_batches = math.ceil(total / self.batch_size)
        
        for i in range(0, total, self.batch_size):
            batch = items[i:i + self.batch_size]
            batches.append(batch)
        
        logger.info(f"创建分片: 总数量 {total}, 分片大小 {self.batch_size}, 总分片数 {total_batches}")
        return batches
    
    def aggregate_batches(self, batch_dir: str, output_file: str) -> bool:
        """聚合所有分片数据"""
        try:
            batch_path = Path(batch_dir)
            if not batch_path.exists():
                logger.error(f"分片目录不存在: {batch_dir}")
                return False
            
            # 查找所有分片文件
            batch_files = sorted(batch_path.glob("batch_*_metrics.json"))
            if not batch_files:
                logger.error("未找到分片数据文件")
                return False
            
            logger.info(f"找到 {len(batch_files)} 个分片文件")
            
            # 聚合数据
            aggregated_data = {
                "version": "4.3.0",
                "created_at": datetime.now().isoformat(),
                "success": True,
                "total_batches": len(batch_files),
                "data": []
            }
            
            for batch_file in batch_files:
                try:
                    with open(batch_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                    
                    # 提取实际数据
                    if 'data' in batch_data:
                        if isinstance(batch_data['data'], dict) and 'data' in batch_data['data']:
                            # 嵌套结构
                            aggregated_data['data'].extend(batch_data['data']['data'])
                        elif isinstance(batch_data['data'], list):
                            # 直接列表
                            aggregated_data['data'].extend(batch_data['data'])
                    
                except Exception as e:
                    logger.warning(f"读取分片文件失败 {batch_file}: {e}")
                    continue
            
            # 保存聚合数据
            aggregated_data['total_records'] = len(aggregated_data['data'])
            FileManager.save_json(aggregated_data, output_file)
            
            logger.info(f"数据聚合完成: 总记录数 {aggregated_data['total_records']}")
            return True
            
        except Exception as e:
            logger.error(f"聚合分片数据失败: {e}")
            return False


class DataValidator:
    """数据验证器"""
    
    @staticmethod
    def validate_facilities(response: FacilityResponse) -> bool:
        """验证设施数据"""
        if not response.success:
            error_msg = response.error or "未知错误"
            raise ValueError(f"设施数据获取失败: {error_msg}")
        
        if not response.data:
            logger.warning("设施数据为空")
        
        data_list = response.data if isinstance(response.data, list) else []
        logger.info(f"设施数据验证成功，包含 {len(data_list)} 条记录")
        return True
    
    @staticmethod
    def validate_metrics(response: TimeSeriesResponse) -> bool:
        """验证指标数据"""
        if not response.success:
            error_msg = response.error or "未知错误"
            raise ValueError(f"指标数据获取失败: {error_msg}")
        
        data_list = response.data if isinstance(response.data, list) else []
        logger.info(f"指标数据验证成功，包含 {len(data_list)} 条记录")
        return True
    
    @staticmethod
    def validate_market(response: TimeSeriesResponse) -> bool:
        """验证市场数据"""
        if not response.success:
            error_msg = response.error or "未知错误"
            raise ValueError(f"市场数据获取失败: {error_msg}")
        
        data_list = response.data if isinstance(response.data, list) else []
        logger.info(f"市场数据验证成功，包含 {len(data_list)} 条记录")
        return True


class FileManager:
    """文件管理器"""
    
    @staticmethod
    def save_json(data: Dict[str, Any], filepath: str, indent: int = 2) -> None:
        """保存 JSON 数据到文件（原子写入）"""
        file_path = Path(filepath)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_filepath = f"{filepath}.tmp"
        
        try:
            # 自定义 JSON 序列化器
            def json_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)
            
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=indent, default=json_serializer)
            
            # 原子重命名
            os.rename(temp_filepath, filepath)
            
            file_size = os.path.getsize(filepath)
            logger.info(f"数据已保存: {filepath}, 文件大小: {file_size} 字节")
            
        except Exception as e:
            # 清理临时文件
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
            logger.error(f"文件保存失败 {filepath}: {e}")
            raise
    
    @staticmethod
    def ensure_directory(directory: str) -> None:
        """确保目录存在"""
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any]:
        """加载 JSON 文件"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)


def _parse_data_interval(interval_str: str) -> DataInterval:
    """解析字符串为 DataInterval Literal 类型"""
    # DataInterval 是 Literal['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
    # 在运行时，Literal 类型就是字符串值，直接返回即可
    # 验证字符串是否是有效的 Literal 值
    valid_intervals = ['5m', '1h', '1d', '7d', '1M', '3M', 'season', '1y', 'fy']
    
    if interval_str in valid_intervals:
        return interval_str  # type: ignore[return-value]
    else:
        # 如果不在有效值中，使用默认值 '5m'
        logger.warning(f"无效的间隔值 '{interval_str}'，使用默认值 '5m'")
        return '5m'  # type: ignore[return-value]


def fetch_and_save_network_data(
    client: OEClient,
    network_id: List[str],
    output_file: str
) -> None:
    """获取并保存网络设施数据"""
    logger.info(f"步骤1: 获取网络设施数据 (network_id: {network_id})")
    
    response = client.get_facilities(network_id=network_id)
    DataValidator.validate_facilities(response)
    
    data = DataConverter.facility_response_to_dict(response)
    data = DataConverter.add_metadata(data, "network")
    
    FileManager.save_json(data, output_file)
    logger.info("网络设施数据保存成功")


def fetch_and_save_metrics_batch(
    client: OEClient,
    facility_codes: List[str],
    network_code: NetworkCode,
    metrics: List[DataMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    batch_size: int,
    batch_delay: float
) -> None:
    """批量获取并保存设施指标数据"""
    logger.info(f"步骤2: 批量获取设施指标数据 (共 {len(facility_codes)} 个设施)")
    
    batch_handler = BatchHandler(batch_size, batch_delay)
    batches = batch_handler.create_batches(facility_codes)
    total_batches = len(batches)
    
    # 确保输出目录存在
    FileManager.ensure_directory("data/batch_data")
    
    successful_batches = 0
    failed_batches = 0
    
    for idx, batch_codes in enumerate(batches):
        logger.info(f"处理分片 {idx + 1}/{total_batches}: {len(batch_codes)} 个设施")
        
        try:
            # 调用 SDK 获取数据
            response = client.get_facility_data(
                network_code=network_code,
                facility_code=batch_codes,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
            
            DataValidator.validate_metrics(response)
            
            # 转换为字典并保存
            data = DataConverter.timeseries_response_to_dict(response, "metrics")
            
            batch_filename = f"data/batch_data/batch_{idx + 1:03d}_metrics.json"
            batch_info = {
                "batch_index": idx + 1,
                "facility_codes": batch_codes,
                "timestamp": datetime.now().isoformat(),
                "data": data
            }
            
            FileManager.save_json(batch_info, batch_filename)
            successful_batches += 1
            logger.info(f"分片 {idx + 1} 处理成功")
            
        except Exception as e:
            failed_batches += 1
            logger.error(f"分片 {idx + 1} 处理失败: {e}")
        
        # 批次间延迟（最后一个分片不需要延迟）
        if idx < total_batches - 1:
            logger.info(f"等待 {batch_delay} 秒后处理下一个分片...")
            time.sleep(batch_delay)
    
    logger.info(f"分片处理完成: 成功 {successful_batches}, 失败 {failed_batches}")
    
    # 聚合所有分片数据
    if successful_batches > 0:
        logger.info("开始聚合分片数据...")
        batch_handler.aggregate_batches(
            "data/batch_data",
            "data/facility_metrics.json"
        )
    else:
        logger.error("没有成功处理的分片，跳过数据聚合")


def fetch_and_save_market_data(
    client: OEClient,
    network_code: NetworkCode,
    metrics: List[MarketMetric],
    interval: DataInterval,
    date_start: datetime,
    date_end: datetime,
    primary_grouping: Optional[DataPrimaryGrouping] = None,
    network_region: Optional[str] = None,
    output_file: str = "data/market_data.json"
) -> None:
    """获取并保存市场数据"""
    logger.info(f"步骤3: 获取市场数据 (network_code: {network_code})")
    
    response = client.get_market(
        network_code=network_code,
        metrics=metrics,
        interval=interval,
        date_start=date_start,
        date_end=date_end,
        primary_grouping=primary_grouping,
        network_region=network_region
    )
    
    DataValidator.validate_market(response)
    
    data = DataConverter.timeseries_response_to_dict(response, "market")
    data = DataConverter.add_metadata(data, "market")
    
    FileManager.save_json(data, output_file)
    logger.info("市场数据保存成功")


def main() -> int:
    """主控制流程"""
    logger.info("=" * 60)
    logger.info("OpenElectricity SDK 数据采集程序启动")
    logger.info("=" * 60)
    
    try:
        # 1. 加载配置
        api_key = ConfigLoader.load_api_key()
        base_url = ConfigLoader.load_base_url()
        date_start, date_end = ConfigLoader.load_time_window()
        interval_str = ConfigLoader.load_interval()
        batch_config = ConfigLoader.load_batch_config()
        network_filter = ConfigLoader.load_network_filter()
        status_filter = ConfigLoader.load_status_filter()
        
        logger.info(f"配置加载完成:")
        logger.info(f"  时间范围: {date_start.isoformat()} 至 {date_end.isoformat()}")
        logger.info(f"  数据间隔: {interval_str}")
        logger.info(f"  分片大小: {batch_config['batch_size']}")
        logger.info(f"  批次延迟: {batch_config['batch_delay']} 秒")
        logger.info(f"  网络过滤: {network_filter}")
        logger.info(f"  状态过滤: {status_filter}")
        
        # 2. 初始化 SDK 客户端
        if base_url:
            client = OEClient(api_key=api_key, base_url=base_url)
        else:
            client = OEClient(api_key=api_key)
        
        logger.info("SDK 客户端初始化成功")
        
        # 3. 确保数据目录存在
        FileManager.ensure_directory("data")
        FileManager.ensure_directory("data/batch_data")
        
        success_count = 0
        total_tasks = 3
        
        # 步骤1: 获取网络设施数据
        try:
            fetch_and_save_network_data(
                client=client,
                network_id=["NEM"],
                output_file="data/facilities_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤1失败: {e}", exc_info=True)
        
        # 步骤2: 处理设施指标数据（批量）
        try:
            # 加载设施数据
            facilities_data = FileManager.load_json("data/facilities_data.json")
            facility_response = FacilityResponse.model_validate(facilities_data)
            
            # 提取设施代码
            facility_codes = FacilityCodeExtractor.extract_codes(
                facility_response,
                network_filter=network_filter,
                status_filter=status_filter
            )
            
            if not facility_codes:
                logger.warning("未提取到设施代码，使用默认值")
                facility_codes = ["BAYSW1", "ERARING"]
            
            # 转换枚举类型
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [DataMetric.POWER, DataMetric.EMISSIONS]
            
            # 批量处理
            fetch_and_save_metrics_batch(
                client=client,
                facility_codes=facility_codes,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                batch_size=batch_config['batch_size'],
                batch_delay=batch_config['batch_delay']
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤2失败: {e}", exc_info=True)
        
        # 步骤3: 获取市场数据
        try:
            network_code = "NEM"
            interval = _parse_data_interval(interval_str)
            metrics_list = [MarketMetric.PRICE, MarketMetric.DEMAND]
            
            fetch_and_save_market_data(
                client=client,
                network_code=network_code,
                metrics=metrics_list,
                interval=interval,
                date_start=date_start,
                date_end=date_end,
                primary_grouping=None,
                network_region=None,
                output_file="data/market_data.json"
            )
            success_count += 1
        except Exception as e:
            logger.error(f"步骤3失败: {e}", exc_info=True)
        
        # 结果汇总
        logger.info("=" * 60)
        logger.info("数据采集流程完成")
        logger.info(f"成功完成任务: {success_count}/{total_tasks}")
        logger.info(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        return 0 if success_count == total_tasks else 1
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)


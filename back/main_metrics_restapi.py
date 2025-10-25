#!/usr/bin/env python3
"""
OpenElectricity Metrics REST API 客户端
仿照 SDK 实现方式，使用 session 调用接口获取指标数据并写入文件
"""

import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from aiohttp import ClientSession, ClientResponse
from aiohttp.client_exceptions import ClientError

# 设置环境变量
os.environ["OPENELECTRICITY_API_KEY"] = "oe_3ZVGZZG6UcWimHS6rF7BPK6e"

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


class OpenElectricityError(Exception):
    """OpenElectricity API 基础异常类"""
    pass


class APIError(OpenElectricityError):
    """API 调用异常类"""
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"API Error {status_code}: {detail}")


class MetricsAPIClient:
    """Metrics REST API 客户端"""

    def __init__(self, api_key: str, base_url: str = "https://api.openelectricity.org.au/v4"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/") + "/"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _handle_response(self, response: ClientResponse) -> Dict[str, Any]:
        """处理 API 响应"""
        if not response.ok:
            try:
                error_data = await response.json()
                detail = error_data.get("detail", response.reason)
            except Exception:
                detail = response.reason
            logger.error(f"API 错误: {response.status} - {detail}")
            raise APIError(response.status, detail or "")
        return await response.json()

    async def _async_get_facility_metrics(
        self,
        network_code: str,
        facility_code: str | List[str],
        metrics: List[str],
        interval: str | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
    ) -> Dict[str, Any]:
        """异步获取设施指标数据 - 参考SDK实现"""
        logger.debug(
            "Getting facility metrics for %s/%s (metrics: %s, interval: %s)",
            network_code,
            facility_code,
            metrics,
            interval,
        )
        
        # 构建请求参数 - 完全参考SDK实现
        params = {
            "facility_code": facility_code,
            "metrics": metrics,
            "interval": interval,
            "date_start": date_start.isoformat() if date_start else None,
            "date_end": date_end.isoformat() if date_end else None,
        }
        # 移除None值 - 参考SDK实现
        params = {k: v for k, v in params.items() if v is not None}
        logger.debug("Request parameters: %s", params)

        async with ClientSession(base_url=self.base_url, headers=self.headers) as session:
            async with session.get(f"/data/facilities/{network_code}", params=params) as response:
                return await self._handle_response(response)

    def get_facility_metrics(
        self,
        network_code: str,
        facility_code: str | List[str],
        metrics: List[str],
        interval: str | None = None,
        date_start: datetime | None = None,
        date_end: datetime | None = None,
    ) -> Dict[str, Any]:
        """同步获取设施指标数据接口"""
        async def _run():
            return await self._async_get_facility_metrics(
                network_code=network_code,
                facility_code=facility_code,
                metrics=metrics,
                interval=interval,
                date_start=date_start,
                date_end=date_end
            )
        return asyncio.run(_run())


class MetricsDataProcessor:
    """指标数据处理和验证类"""

    @staticmethod
    def validate_metrics_response(data: Dict[str, Any]) -> bool:
        """验证指标API响应数据格式"""
        try:
            # 检查基本响应结构
            if not isinstance(data, dict):
                logger.error("响应数据不是字典格式")
                return False
            
            # 检查是否有数据字段
            if 'data' not in data:
                logger.error("响应数据缺少data字段")
                return False
            
            data_list = data.get('data', [])
            if not isinstance(data_list, list):
                logger.error("data字段不是列表格式")
                return False
            
            logger.info(f"指标数据验证成功，包含 {len(data_list)} 条记录")
            return True
        except Exception as e:
            logger.error(f"指标数据验证失败: {e}")
            return False

    @staticmethod
    def transform_metrics_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换和标准化指标数据格式"""
        try:
            transformed_data = raw_data.copy()
            transformed_data['processed_at'] = datetime.now().isoformat()
            if 'total_records' not in transformed_data:
                transformed_data['total_records'] = len(transformed_data.get('data', []))
            logger.info("指标数据转换完成")
            return transformed_data
        except Exception as e:
            logger.error(f"指标数据转换失败: {e}")
            raise


class MetricsFileManager:
    """指标文件操作管理类"""

    @staticmethod
    def save_metrics_to_file(data: Dict[str, Any], filename: str = 'facility_metrics.json') -> bool:
        """将指标数据保存为 JSON 文件"""
        try:
            os.makedirs('data', exist_ok=True)
            filepath = f'data/{filename}'
            temp_filepath = f'{filepath}.tmp'
            
            # 自定义JSON序列化器，处理datetime对象
            def json_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)
            
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)
            os.rename(temp_filepath, filepath)
            logger.info(f"指标数据已成功保存到文件: {filepath}")
            file_size = os.path.getsize(filepath)
            logger.info(f"文件大小: {file_size} 字节")
            return True
        except Exception as e:
            logger.error(f"指标数据文件保存失败: {e}")
            temp_filepath = f'data/{filename}.tmp'
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
            return False


def fetch_facility_metrics(
    facility_codes: List[str],
    network_code: str = "NEM",
    metrics: List[str] = ["power", "emissions"],
    interval: str = "5m",
    date_start: Optional[datetime] = None,
    date_end: Optional[datetime] = None
) -> Optional[Dict[str, Any]]:
    """调用 OpenElectricity API 获取设施指标数据"""
    try:
        logger.info(f"开始获取设施 {facility_codes} 的指标数据...")
        logger.info(f"网络代码: {network_code}, 指标: {metrics}")
        logger.info(f"数据间隔: {interval}")
        if date_start:
            logger.info(f"开始日期: {date_start.strftime('%Y-%m-%d %H:%M:%S')}")
        if date_end:
            logger.info(f"结束日期: {date_end.strftime('%Y-%m-%d %H:%M:%S')}")
        
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            logger.error("未找到 API 密钥，请设置 OPENELECTRICITY_API_KEY 环境变量")
            return None
        
        client = MetricsAPIClient(api_key=api_key)
        logger.info("正在连接OpenElectricity API...")
        
        response = client.get_facility_metrics(
            network_code=network_code,
            facility_code=facility_codes,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end
        )
        
        logger.info("API调用成功")
        logger.info(f"成功获取数据，记录总数: {len(response.get('data', []))}")
        return response
        
    except APIError as e:
        logger.error(f"API 调用失败: {e}")
        return None
    except ClientError as e:
        logger.error(f"网络请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"未知错误: {e}")
        return None


def process_metrics_response(response_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """处理和验证指标API响应数据"""
    try:
        if not MetricsDataProcessor.validate_metrics_response(response_data):
            return None
        processed_data = MetricsDataProcessor.transform_metrics_data(response_data)
        return processed_data
    except Exception as e:
        logger.error(f"指标数据处理失败: {e}")
        return None


def save_metrics_to_file(data: Dict[str, Any], filename: str = 'facility_metrics.json') -> bool:
    """将指标数据保存为 JSON 文件"""
    return MetricsFileManager.save_metrics_to_file(data, filename)


def main():
    """主控制函数，协调整个指标数据获取流程"""
    logger.info("=" * 50)
    logger.info("OpenElectricity Metrics REST API 数据获取工具启动")
    logger.info("=" * 50)

    # 配置参数
    facility_codes = ["BAYSW1", "ERARING"]  # 使用示例中的设施代码
    network_code = "NEM"
    metrics = ["power", "emissions"]  # 使用字符串格式
    interval = "5m"  # 使用示例中的间隔
    date_start = datetime(2024, 1, 1)  # 使用示例中的日期
    date_end = datetime(2024, 1, 2)

    try:
        logger.info("步骤1: 调用 API 获取指标数据")
        api_data = fetch_facility_metrics(
            facility_codes=facility_codes,
            network_code=network_code,
            metrics=metrics,
            interval=interval,
            date_start=date_start,
            date_end=date_end
        )
        if api_data is None:
            logger.error("API 调用失败，程序退出")
            return False

        logger.info("步骤2: 处理指标响应数据")
        processed_data = process_metrics_response(api_data)
        if processed_data is None:
            logger.error("指标数据处理失败，程序退出")
            return False

        logger.info("步骤3: 保存指标数据到文件")
        save_success = save_metrics_to_file(processed_data)
        if not save_success:
            logger.error("指标数据文件保存失败，程序退出")
            return False

        logger.info("=" * 50)
        logger.info("指标数据获取完成！")
        logger.info(f"处理时间: {processed_data.get('processed_at', '未知')}")
        logger.info(f"记录总数: {processed_data.get('total_records', '未知')}")
        logger.info(f"数据记录数: {len(processed_data.get('data', []))}")
        logger.info("=" * 50)
        return True
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

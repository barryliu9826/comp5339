#!/usr/bin/env python3
"""
OpenElectricity REST API 客户端
仿照 SDK 实现方式，使用 session 调用接口获取数据并写入文件
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


class OpenElectricityClient:
    """OpenElectricity REST API 客户端"""

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

    async def _async_get_facilities(self, network_id: List[str] = None) -> Dict[str, Any]:
        """异步获取设施数据"""
        params = {"network_id": network_id} if network_id else {}
        async with ClientSession(base_url=self.base_url, headers=self.headers) as session:
            async with session.get("/facilities/", params=params) as response:
                return await self._handle_response(response)

    def get_facilities(self, network_id: List[str] = None) -> Dict[str, Any]:
        """同步获取设施数据接口"""
        async def _run():
            return await self._async_get_facilities(network_id=network_id)
        return asyncio.run(_run())


class DataProcessor:
    """数据处理和验证类"""

    @staticmethod
    def validate_response(data: Dict[str, Any]) -> bool:
        """验证 API 响应数据格式"""
        try:
            required_fields = ['version', 'created_at', 'success', 'data']
            for field in required_fields:
                if field not in data:
                    logger.error(f"响应数据缺少必需字段: {field}")
                    return False
            if not data.get('success', False):
                error_msg = data.get('error', '未知错误')
                logger.error(f"API 返回错误: {error_msg}")
                return False
            data_list = data.get('data', [])
            if not isinstance(data_list, list):
                logger.error("data 字段不是列表格式")
                return False
            logger.info(f"数据验证成功，包含 {len(data_list)} 条设施记录")
            return True
        except Exception as e:
            logger.error(f"数据验证失败: {e}")
            return False

    @staticmethod
    def transform_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """转换和标准化数据格式"""
        try:
            transformed_data = raw_data.copy()
            transformed_data['processed_at'] = datetime.now().isoformat()
            if 'total_records' not in transformed_data:
                transformed_data['total_records'] = len(transformed_data.get('data', []))
            logger.info("数据转换完成")
            return transformed_data
        except Exception as e:
            logger.error(f"数据转换失败: {e}")
            raise


class FileManager:
    """文件操作管理类"""

    @staticmethod
    def save_to_file(data: Dict[str, Any], filename: str = 'facilities_data.json') -> bool:
        """将数据保存为 JSON 文件"""
        try:
            os.makedirs('data', exist_ok=True)
            filepath = f'data/{filename}'
            temp_filepath = f'{filepath}.tmp'
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.rename(temp_filepath, filepath)
            logger.info(f"数据已成功保存到文件: {filepath}")
            file_size = os.path.getsize(filepath)
            logger.info(f"文件大小: {file_size} 字节")
            return True
        except Exception as e:
            logger.error(f"文件保存失败: {e}")
            temp_filepath = f'data/{filename}.tmp'
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
            return False


def fetch_facilities_data() -> Optional[Dict[str, Any]]:
    """调用 OpenElectricity API 获取设施数据"""
    try:
        logger.info("开始调用 OpenElectricity API...")
        api_key = os.getenv("OPENELECTRICITY_API_KEY")
        if not api_key:
            logger.error("未找到 API 密钥，请设置 OPENELECTRICITY_API_KEY 环境变量")
            return None
        client = OpenElectricityClient(api_key=api_key)
        logger.info("正在获取 NEM 网络设施数据...")
        response = client.get_facilities(network_id=["NEM"])
        logger.info("API 调用成功")
        logger.info(f"成功获取数据，设施总数: {len(response.get('data', []))}")
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


def process_api_response(response_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """处理和验证 API 响应数据"""
    try:
        if not DataProcessor.validate_response(response_data):
            return None
        processed_data = DataProcessor.transform_data(response_data)
        return processed_data
    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        return None


def save_data_to_file(data: Dict[str, Any], filename: str = 'facilities_data.json') -> bool:
    """将数据保存为 JSON 文件"""
    return FileManager.save_to_file(data, filename)


def main():
    """主控制函数，协调整个数据获取流程"""
    logger.info("=" * 50)
    logger.info("OpenElectricity REST API 数据获取工具启动")
    logger.info("=" * 50)

    try:
        logger.info("步骤1: 调用 API 获取数据")
        api_data = fetch_facilities_data()
        if api_data is None:
            logger.error("API 调用失败，程序退出")
            return False

        logger.info("步骤2: 处理响应数据")
        processed_data = process_api_response(api_data)
        if processed_data is None:
            logger.error("数据处理失败，程序退出")
            return False

        logger.info("步骤3: 保存数据到文件")
        save_success = save_data_to_file(processed_data)
        if not save_success:
            logger.error("文件保存失败，程序退出")
            return False

        logger.info("=" * 50)
        logger.info("数据获取完成！")
        logger.info(f"API 版本: {processed_data.get('version', '未知')}")
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

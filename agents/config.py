"""
使用 dotenv 加载环境变量配置 API Token，其他参数可动态配置
"""
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# LLM 模型配置
class LLMConfig:
    """LLM 配置类"""
    
    def __init__(self, 
                 provider: str = 'siliconflow',
                 model: str = 'deepseek-ai/DeepSeek-V3.2',
                 base_url: str = None,
                 temperature: float = 0.0,
                 max_retries: int = 3,
                 timeout: int = 60):
        """
        初始化 LLM 配置
        
        Args:
            provider: 模型厂商 (openai, anthropic, zhipu, deepseek 等)
            model: 模型名称
            base_url: API 基础路径（如果为 None 则使用默认值）
            temperature: 温度参数
            max_retries: 最大重试次数
            timeout: 超时时间（秒）
        """
        self.provider = provider
        self.model = model
        self.base_url = base_url or self._get_default_base_url(provider)
        self.api_key = self._get_api_key(provider)
        self.temperature = temperature
        self.max_retries = max_retries
        self.timeout = timeout
    
    def _get_api_key(self, provider: str) -> str:
        """
        根据模型厂商获取对应的 API Key
        
        Args:
            provider: 模型厂商
            
        Returns:
            str: API Key
        """
        # 支持的模型厂商及其对应的环境变量
        provider_env_map = {
            'openai': 'OPENAI_API_KEY',
            'anthropic': 'ANTHROPIC_API_KEY',
            'zhipu': 'ZHIPU_API_KEY',
            'deepseek': 'DEEPSEEK_API_KEY',
            'dashscope': 'DASHSCOPE_API_KEY',
            'moonshot': 'MOONSHOT_API_KEY',
            'baichuan': 'BAICHUAN_API_KEY',
            'minimax': 'MINIMAX_API_KEY',
            'siliconflow': 'SILICONFLOW_API_KEY'
        }
        
        env_key = provider_env_map.get(provider.lower(), 'LLM_API_KEY')
        api_key = os.getenv(env_key, '')
        
        if not api_key:
            logger.warning(f"未找到 {provider} 的 API Key (环境变量: {env_key})")
        
        return api_key
    
    def _get_default_base_url(self, provider: str) -> str:
        """
        获取模型厂商的默认 Base URL
        
        Args:
            provider: 模型厂商
            
        Returns:
            str: 默认 Base URL
        """
        default_urls = {
            'openai': 'https://api.openai.com/v1',
            'anthropic': 'https://api.anthropic.com/v1',
            'zhipu': 'https://open.bigmodel.cn/api/paas/v4',
            'deepseek': 'https://api.deepseek.com/v1',
            'dashscope': 'https://dashscope.aliyuncs.com/compatible-mode/v1',
            'moonshot': 'https://api.moonshot.cn/v1',
            'baichuan': 'https://api.baichuan-ai.com/v1',
            'minimax': 'https://api.minimax.chat/v1',
            'siliconflow': 'https://api.siliconflow.cn/v1'
        }
        
        return default_urls.get(provider.lower(), 'https://api.openai.com/v1')
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'provider': self.provider,
            'model': self.model,
            'base_url': self.base_url,
            'api_key': self.api_key,
            'temperature': self.temperature,
            'max_retries': self.max_retries,
            'timeout': self.timeout
        }

# 日志配置
class LoggingConfig:
    """日志配置类"""
    
    def __init__(self,
                 level: str = 'INFO',
                 format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
        """
        初始化日志配置
        
        Args:
            level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format: 日志格式
        """
        self.level = level
        self.format = format
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'level': self.level,
            'format': self.format
        }


# 默认配置实例
default_llm_config = LLMConfig()
default_logging_config = LoggingConfig()


def get_llm_config(**kwargs) -> LLMConfig:
    """
    获取 LLM 配置
    
    Args:
        **kwargs: 动态配置参数，会覆盖默认值
        
    Returns:
        LLMConfig: LLM 配置实例
    """
    config = LLMConfig(**kwargs)
    return config

def validate_config(llm_config: LLMConfig = None) -> bool:
    """
    验证配置是否有效
    
    Args:
        llm_config: LLM 配置实例
        
    Returns:
        bool: 配置是否有效
    """
    if llm_config is None:
        llm_config = default_llm_config
    
    # 验证 LLM 配置
    if not llm_config.api_key:
        print("警告: 未设置 LLM_API_KEY 环境变量")
        return False
    
    if not llm_config.model:
        print("警告: 未设置模型名称")
        return False
    
    return True


def get_logging_config(**kwargs) -> LoggingConfig:
    """
    获取日志配置
    
    Args:
        **kwargs: 动态配置参数，会覆盖默认值
        
    Returns:
        LoggingConfig: 日志配置实例
    """
    config = LoggingConfig(**kwargs)
    return config

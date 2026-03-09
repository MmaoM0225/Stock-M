"""
使用 dotenv 加载环境变量配置 API Token，其他参数可动态配置
"""
import os
from re import T
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

MACRO_DAILY_LOOKBACK = 60   # 日线回溯天数
MACRO_MONTH_LOOKBACK = 12   # 月度回溯月数（LPR/CPI/社融）
# Markdown 报告生成：由单独节点执行，configurable.macro_config 可覆盖
MACRO_GENERATE_MARKDOWN = True  # 是否生成 macro Markdown 报告
MACRO_USE_LLM_FOR_MARKDOWN = False  # 是否用 LLM 润色 MD（多一次调用、耗时增加）
NEWS_GENERATE_MARKDOWN = False  # 是否生成 news Markdown 报告
NEWS_USE_LLM_FOR_MARKDOWN = False  # 是否用 LLM 润色 news MD
# 策略经理配置
STRATEGY_GENERATE_MARKDOWN = True  # 是否生成 strategy Markdown 报告
STRATEGY_USE_LLM_FOR_MARKDOWN = False  # 是否用 LLM 润色 strategy MD
# 宏观经济分析师配置
MACRO_USE_US_STOCK_TREND = False  # 是否纳入美股趋势分析
# 国内市场分析默认指数：指数名称、指数代码、指数描述
MACRO_DEFAULT_INDEX_CODES = [
    {
        "name": "上证综指",
        "code": "000001.SH",
        "description": "上海证券交易所综合股价指数，反映沪市整体表现",
    },
    {
        "name": "沪深300",
        "code": "000300.SH",
        "description": "由沪深两市最具代表性的300只股票组成，反映A股市场整体走势",
    },
    {
        "name": "中证500",
        "code": "000905.SH",
        "description": "剔除沪深300后、总市值排名靠前的500只股票，代表中盘股表现",
    },
    {
        "name": "中证1000",
        "code": "000852.SH",
        "description": "剔除沪深300和中证500后、总市值排名靠前的1000只股票，代表小盘股表现",
    },
    {
        "name": "创业板指",
        "code": "399006.SZ",
        "description": "深交所创业板最具代表性的100只股票，反映成长型公司整体表现",
    },
]
# 大宗商品：name、code、description、source。source 区分数据源，合约代码需要及时更新
# source: "sge" 上海黄金交易所现货 -> fetch_sge_daily；"fut" 期货 -> fetch_fut_daily
MACRO_DEFAULT_COMMODITY_CODES = [
    {
        "name": "黄金",
        "code": "Au99.99",
        "description": "上海黄金交易所现货黄金，避险资产、通胀预期指标",
        "source": "sge",
    },
    {
        "name": "原油",
        "code": "SC2604.INE",
        "description": "INE 原油期货主力合约，能源价格、全球供需与地缘局势",
        "source": "fut",
    },
    {
        "name": "铜",
        "code": "CU2604.SHF",
        "description": "沪铜期货，工业金属代表，经济景气度指标",
        "source": "fut",
    },
    {
        "name": "螺纹钢",
        "code": "RB2604.SHF",
        "description": "上期所螺纹钢期货，基建、地产、建筑活动指标",
        "source": "fut",
    },
    {
        "name": "白银",
        "code": "AG2606.SHF",
        "description": "沪银期货，贵金属，与黄金联动、通胀预期",
        "source": "fut",
    },
    {
        "name": "铁矿石",
        "code": "I2605.DCE",
        "description": "大商所铁矿石期货，钢铁、基建投资需求",
        "source": "fut",
    },
]


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

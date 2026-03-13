"""
LLM 回调：将无意义的 HTTP POST 200 OK 日志替换为「LLM调用成功，正在处理 XXX」。
"""
import logging
import re
from typing import Any, Optional

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult

logger = logging.getLogger(__name__)

# LangGraph/LangChain 内部的步骤名（如 seq:step:2），不展示给用户
_INTERNAL_RUN_NAME_PATTERN = re.compile(r"^seq:step:\d+$|^RunnableSequence$|^step:\d+$", re.I)


def _normalize_run_name(raw: Optional[str]) -> str:
    """若为框架内部步骤名则返回「LLM 请求」，否则返回原样。"""
    if not raw or not raw.strip():
        return "LLM 请求"
    s = raw.strip()
    if _INTERNAL_RUN_NAME_PATTERN.match(s):
        return "LLM 请求"
    return s


class LLMLoggingCallback(BaseCallbackHandler):
    """LLM 结束时打 INFO：LLM调用成功，正在处理 {run_name}。内部步骤名（如 seq:step:2）会显示为「LLM 请求」。"""

    @property
    def always_verbose(self) -> bool:
        return False

    def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: Optional[list] = None,
        **kwargs: Any,
    ) -> None:
        raw = kwargs.get("run_name") or (tags[0] if tags else None)
        run_name = _normalize_run_name(str(raw) if raw is not None else None)
        logger.info("LLM调用成功，正在处理 %s", run_name)


def get_llm_callbacks() -> list:
    """返回供 ChatOpenAI(callbacks=...) 使用的回调列表。"""
    return [LLMLoggingCallback()]

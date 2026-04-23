from functools import lru_cache
from typing import Generator

from fastapi import Depends
from langchain_openai import ChatOpenAI
from sqlalchemy.orm import Session

from agents.callbacks import get_llm_callbacks
from agents.config import get_llm_config, validate_config
from database.config import get_session

from api.services.agent_service import AgentService
from api.services.data_service import DataService
from api.services.home_service import HomeService


@lru_cache()
def get_llm() -> ChatOpenAI:
    llm_config = get_llm_config()
    if not validate_config(llm_config):
        raise RuntimeError("LLM 配置无效，请检查环境变量")
    return ChatOpenAI(
        base_url=llm_config.base_url,
        api_key=llm_config.api_key,
        model=llm_config.model,
        temperature=llm_config.temperature,
        max_retries=llm_config.max_retries,
        timeout=llm_config.timeout,
        callbacks=get_llm_callbacks(),
    )


def get_db() -> Generator[Session, None, None]:
    session = get_session()
    try:
        yield session
    finally:
        session.close()


def get_agent_service(llm: ChatOpenAI = Depends(get_llm)) -> AgentService:
    return AgentService(llm=llm)


def get_data_service(db: Session = Depends(get_db)) -> DataService:
    return DataService(db=db)


def get_home_service(db: Session = Depends(get_db)) -> HomeService:
    return HomeService(db=db)


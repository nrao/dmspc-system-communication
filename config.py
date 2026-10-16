#Share Common Settings and Environment Variables with FastAPI
import os
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.environ.get(".env"),
        arbitrary_types_allowed=True,
        extra="allow",
    )

    SECRET_KEY: str


settings = Settings() 
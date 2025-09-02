import os
from dagster import ConfigurableResource, Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import httpx
from typing import Optional

class DatabaseConfig(Config):
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class FonteDatabaseResource(ConfigurableResource):
    
    config: DatabaseConfig
    
    def get_engine(self):
        return create_engine(self.config.connection_string)
    
    def get_session(self):
        engine = self.get_engine()
        return sessionmaker(bind=engine)()

class AlvoDatabaseResource(ConfigurableResource):
    
    config: DatabaseConfig
    
    def get_engine(self):
        return create_engine(self.config.connection_string)
    
    def get_session(self):
        engine = self.get_engine()
        return sessionmaker(bind=engine)()

class APIConfig(Config):
    base_url: str
    timeout: int = 30

class APIResource(ConfigurableResource):
    
    config: APIConfig
    
    def get_client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.config.base_url,
            timeout=self.config.timeout
        )
    
    def health_check(self) -> bool:
        try:
            with self.get_client() as client:
                response = client.get("/health")
                return response.status_code == 200
        except Exception:
            return False

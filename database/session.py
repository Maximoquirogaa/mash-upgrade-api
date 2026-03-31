import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://postgres:[YOUR-PASSWORD]@db.eslzcyrfedgnboqmjayv.supabase.co:5432/postgres"

class DatabaseManager:
    def __init__(self, connection_url: str):
        self.engine = create_engine(connection_url, pool_pre_ping=True)
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def get_session(self):
        """Retorna una nueva sesión de base de datos."""
        return self.SessionLocal()

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# # En producción, esta URL se lee de un archivo .env
# # Estructura: postgresql://[user]:[password]@[host]:[port]/[db_name]
DATABASE_URL = "postgresql://postgres:[YOUR-PASSWORD]@db.eslzcyrfedgnboqmjayv.supabase.co:5432/postgres"

class DatabaseManager:
    def __init__(self, connection_url: str):
        # # El 'engine' es el corazón de la conexión
        self.engine = create_engine(connection_url, pool_pre_ping=True)
        # # 'sessionmaker' crea sesiones de trabajo individuales
        self.SessionLocal = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)

    def get_session(self):
        """Retorna una nueva sesión de base de datos."""
        return self.SessionLocal()
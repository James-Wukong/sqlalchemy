from src.database import engine
from src.models.mapped_models import Base
from sqlalchemy.orm import Session
import sqlalchemy as sa
engine = engine.sql_engine()

def create_model_tables():
    Base.metadata.create_all(bind=engine)


with Session(bind=engine) as session:
    pass

if __name__ == '__main__':
    create_model_tables()
    # print(sa.__version__)
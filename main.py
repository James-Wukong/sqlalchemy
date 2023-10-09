from src.database import engine
from src.models.mapped_models import Base
from sqlalchemy.orm import Session
import sqlalchemy as sa
import populate as etl

engine = engine.sql_engine()

def create_model_tables():
    Base.metadata.create_all(bind=engine)


with Session(bind=engine) as session:
    pass

if __name__ == '__main__':
    # create_model_tables()
    # etl.dump_orders_db()
    # etl.insert_metadatas()
    # etl.etl_country()
    # etl.etl_people()
    # etl.etl_state()
    # etl.etl_city()
    # etl.etl_address()
    # etl.etl_category()
    # etl.etl_order_status()
    # etl.etl_segment()
    # etl.etl_customer()
    # etl.etl_address_customer()
    pass
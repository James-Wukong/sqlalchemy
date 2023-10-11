from src.database import engine
from src.models.mapped_models import Base
from sqlalchemy.orm import Session
import sqlalchemy as sa
import populate as etl
import cleansing as clean

engine = engine.sql_engine()

def create_model_tables():
    Base.metadata.create_all(bind=engine)


with Session(bind=engine) as session:
    pass

if __name__ == '__main__':
    # these exections need be carried out in sequence
    # first of all, create tables from models
    create_model_tables()

    # import data into superstore_orders table
    # etl.dump_orders_db()

    # clean products data
    # clean.clean_products_v1()
    # clean.fillin_order_status()

    # create metadata records
    # etl.insert_metadatas()

    # Extract Transform and Load data to tables, in sequence
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
    # etl.etl_product()
    # etl.etl_orders()
    # etl.etl_product_order()
import os
import pandas as pd
from sqlalchemy.orm import Session
import sqlalchemy as sa

from src.constants import ROOT_DIR
from src.database import engine as db_engine
from src.models import mapped_models as mm

data_file = os.path.join(ROOT_DIR, 'data', 'Sample - Superstore.xls')
engine = db_engine.sql_engine()

df_orders = pd.read_excel(data_file, sheet_name='Orders')
df_people = pd.read_excel(data_file, sheet_name='People')
df_returns = pd.read_excel(data_file, sheet_name='Returns')

# dump orders data into superstore_orders table
def dump_orders_db():
    table_name = 'superstore_orders'
    df_orders['Postal Code'] = df_orders['Postal Code'].fillna('52601')
    # print(df_orders.head(2))
    df_orders_insert = df_orders.rename(columns={
            'Row ID': 'row_id', 'Order ID':'order_no', 'Order Date':'order_at', \
            'Ship Date':'ship_at', 'Ship Mode':'ship_mode', \
            'Customer ID':'customer_no', 'Customer Name':'customer_name',\
            'Segment':'segment', 'Country/Region':'country', 'City':'city', \
            'State':'state', 'Postal Code':'post_code', 'Region':'region', \
            'Product ID':'product_no', 'Category':'category', \
            'Sub-Category':'sub_cate', 'Product Name':'product_name', \
            'Sales':'sales', 'Quantity':'quantity', 'Discount':'discount', \
            'Profit':'profit'
    })
    df_order_insert = df_orders_insert.to_dict(orient='records')
    stmt = sa.text(f'INSERT INTO {table_name} (row_id, order_no, order_at, ship_at,\
                ship_mode, customer_no, customer_name, segment, country, city,\
                state, post_code, region, product_no, category, sub_cate, \
                product_name, sales, quantity, discount, profit) \
            values (:row_id, :order_no, :order_at, :ship_at, :ship_mode, :customer_no,\
                :customer_name, :segment, :country, :city, :state, :post_code, :region,\
                :product_no, :category, :sub_cate, :product_name, :sales, :quantity, :discount, :profit)')
    with Session(bind=engine) as session:
        session.execute(stmt, df_order_insert)
        session.commit()

# dump metadata table with descriptions of tables
def insert_metadatas():
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Metadata), [
                {'table_name': 'address_customers', 'column_name': '', 'data_type': '',
                    'description': 'this table stores customer_id and address_id', 
                    'constraints': 'combination of customer_id and address_id is unqiue key in this table',
                    'relationships': 'references to custoemrs table and addresses table'},
                {'table_name': 'address_customers', 'column_name': 'id', 'data_type': 'unsigned int',
                    'description': 'primary key of the table', 
                    'constraints': 'nullable=false, autoincrement',
                    'relationships': ''},
                {'table_name': 'address_customers', 'column_name': 'customer_id', 'data_type': 'unsigned int',
                    'description': 'foreign key of the table', 
                    'constraints': 'nullable=false',
                    'relationships': 'references to customers table'},
                {'table_name': 'address_customers', 'column_name': 'address_id', 'data_type': 'unsigned int',
                    'description': 'foreign key of the table', 
                    'constraints': 'nullable=false',
                    'relationships': 'references to addresses table'},
            ],
        )
        session.commit()

# Extract Transforn and Load  country into countries table
def etl_country():
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Country), [
                {'name': df_orders['Country/Region'].unique()[0]}
            ]
        )
        session.commit()

# Extract Transforn and Load  people into employees and regions table
def etl_people():
    df_people['id'] = range(1, 1 + len(df_people))
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Region), [
                {'id': id, 'name': region} for id, region in zip(df_people['id'].tolist(), df_people['Region'].tolist())
            ]
        )
        session.execute(
            sa.insert(mm.Employee), [
                {'first_name': name.split()[1], 'last_name': name.split()[0], 'region_id': id} \
                    for id, name in zip(df_people['id'].tolist(), df_people['Regional Manager'].tolist())
            ]
        )
        
        session.commit()

# Extract Transforn and Load state data into states table
def etl_state():
    subq = (
        sa.select(mm.SupserstoreOrder.country, mm.SupserstoreOrder.state, mm.SupserstoreOrder.region)
        .group_by(mm.SupserstoreOrder.country, mm.SupserstoreOrder.state)
        .subquery()
    )

    stmt = sa.select(mm.Country.id, subq.c.state, mm.Region.id).join_from(
        subq, mm.Country, mm.Country.name == subq.c.country
    ).join(
        mm.Region, subq.c.region == mm.Region.name
    )
    # with Session(bind=engine) as session:
    #     for id, state in session.execute(stmt):
    #         session.execute(
    #             sa.insert(mm.State).values('name')
    #         )
    #     session.commit()
    print(stmt)

# for id, state in etl_people():
#     print(id, state)
etl_state()
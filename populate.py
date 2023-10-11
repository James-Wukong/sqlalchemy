import os
import pandas as pd
from sqlalchemy.orm import Session
import sqlalchemy as sa

from src.constants import ROOT_DIR
from src.database import engine as db_engine
from src.models import mapped_models as mm
from src.helpers import parse_name, unit_price

data_file = os.path.join(ROOT_DIR, 'data', 'Sample - Superstore.xls')
engine = db_engine.sql_engine()

df_orders = pd.read_excel(data_file, sheet_name='Orders')
df_people = pd.read_excel(data_file, sheet_name='People')
df_returns = pd.read_excel(data_file, sheet_name='Returns')

# dump orders data into superstore_orders table
def dump_orders_db():
    table_name = 'superstore_orders'
    # pick random post code from city 'Burlington'
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
                {'id': id, 'name': region}
                    for id, region in zip(df_people['id'].tolist(), df_people['Region'].tolist())
            ]
        )
        session.execute(
            sa.insert(mm.Employee), [
                {'first_name': name.split()[1], 'last_name': name.split()[0], 'region_id': id}
                    for id, name in zip(df_people['id'].tolist(), df_people['Regional Manager'].tolist())
            ]
        )
        
        session.commit()

# Extract Transforn and Load state data into states table
def etl_state():
    subq = (
        sa.select(mm.SupserstoreOrder.country, mm.SupserstoreOrder.state, mm.SupserstoreOrder.region)
        .group_by(mm.SupserstoreOrder.country, mm.SupserstoreOrder.state, mm.SupserstoreOrder.region)
        .subquery()
    )

    stmt = sa.select(mm.Country.id, subq.c.state, mm.Region.id.label('region_id')).join_from(
        subq, mm.Country, mm.Country.name == subq.c.country
    ).join(
        mm.Region, subq.c.region == mm.Region.name
    )
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.State), [
                {'name': state, 'country_id': country_id, 'region_id': region_id}
                for country_id, state, region_id in session.execute(stmt)
            ],
        )
        session.commit()

# ETL city data to cities table
def etl_city():
    subq = (sa.select(mm.SupserstoreOrder.state, mm.SupserstoreOrder.city)
        .group_by(mm.SupserstoreOrder.state, mm.SupserstoreOrder.city)
        .subquery()
    )
    stmt = sa.select(mm.State.id.label('state_id'), subq.c.city).join_from(
        subq, mm.State, mm.State.name == subq.c.state
    )

    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.City), [
                {'name': city, 'state_id': state_id}
                for state_id, city in session.execute(stmt)
            ],
        )
        session.commit()

# ETL address data to addresses table
def etl_address():
    subq_state = (sa.select(mm.SupserstoreOrder.state, 
                            mm.SupserstoreOrder.city, 
                            mm.SupserstoreOrder.post_code)
        .group_by(mm.SupserstoreOrder.state, 
                  mm.SupserstoreOrder.city, 
                  mm.SupserstoreOrder.post_code)
        .subquery()
    )
    subq_city = (sa.select(mm.State.id.label('state_id'), 
                           subq_state.c.post_code, 
                           subq_state.c.city).join_from(
        subq_state, mm.State, mm.State.name == subq_state.c.state
    ).subquery())
    stmt = sa.select(mm.City.id.label('city_id'), subq_city.c.post_code).join_from(
        subq_city, mm.City, sa.and_(mm.City.name == subq_city.c.city, 
                                    mm.City.state_id == subq_city.c.state_id)
    )
    
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Address), [
                {'postcode': post_code, 'city_id': city_id}
                for city_id, post_code in session.execute(stmt)
            ],
        )
        session.commit()

# ETL categories into categories table
def etl_category():
    cate_stmt = sa.select(sa.distinct(mm.SupserstoreOrder.category))
    subq = (sa.select(mm.SupserstoreOrder.category, mm.SupserstoreOrder.sub_cate)
        .group_by(mm.SupserstoreOrder.category, mm.SupserstoreOrder.sub_cate)
        .subquery()
    )
    sub_cate_stmt = sa.select(mm.Category.id.label('parent_id'), subq.c.sub_cate).join_from(
        subq, mm.Category, mm.Category.name == subq.c.category
    )
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Category), [
                {'name': name} for name in session.scalars(cate_stmt)
            ],
        )
        session.commit()
        session.execute(
            sa.insert(mm.Category), [
                {'name': sub_cate, 'parent_id': parent_id}
                    for parent_id, sub_cate in session.execute(sub_cate_stmt)
            ],
        )
        session.commit()

    # print(cate_stmt)

# etl order statuses into table
def etl_order_status():
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.OrderStatus), [
                {'name': 'completed'},
                {'name': 'returned'}
            ],
        )
        session.commit()

# etl segment into table
def etl_segment():
    stmt = sa.select(sa.distinct(mm.SupserstoreOrder.segment))
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Segment), [
                {'name': name} for name in session.scalars(stmt)
            ],
        )
        session.commit()

# etl customers into customers table
def etl_customer():
    subq = (sa.select(mm.SupserstoreOrder.customer_no, 
                      mm.SupserstoreOrder.customer_name, 
                      mm.SupserstoreOrder.segment)
        .group_by(mm.SupserstoreOrder.customer_no, 
                  mm.SupserstoreOrder.customer_name, 
                  mm.SupserstoreOrder.segment)
        .subquery()
    )
    stmt = sa.select(mm.Segment.id.label('segment_id'), 
                     subq.c.customer_no, 
                     subq.c.customer_name).join_from(
        subq, mm.Segment, mm.Segment.name == subq.c.segment
    )
    # print(stmt)
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Customer), [
                {'customer_no': customer_no, 'segment_id': segment_id,
                'first_name': parse_name(customer_name)[0],
                'mid_name': parse_name(customer_name)[1],
                'last_name': parse_name(customer_name)[2]}
                for segment_id, customer_no, customer_name in session.execute(stmt)
            ],
        )
        session.commit()

# etl customer address table
def etl_address_customer():
    with Session(bind=engine) as session:
        q = (session.query(sa.distinct(mm.SupserstoreOrder.customer_no), 
                           mm.Customer.id, mm.Address.id)
                .join(mm.Customer, mm.SupserstoreOrder.customer_no == mm.Customer.customer_no)
                .join(mm.Address, mm.SupserstoreOrder.post_code == mm.Address.postcode)
                .all())
        
        session.execute(
            sa.insert(mm.AddressCustomer), [
                {'customer_id': customer_id, 'address_id': address_id}
                    for _, customer_id, address_id in q
            ]
        )
        session.commit()
        
# etl products table
def etl_product():
    subq_id = (sa.select(sa.func.min(mm.SupserstoreOrder.id).label('min_id'))
        .group_by(mm.SupserstoreOrder.product_no, mm.SupserstoreOrder.product_name)
        # .subquery()
    )
    subq_products = (sa.select(mm.SupserstoreOrder.product_no, 
                               mm.SupserstoreOrder.product_name, 
                               mm.SupserstoreOrder.sub_cate, 
                               mm.SupserstoreOrder.sales,
                               mm.SupserstoreOrder.quantity,
                               mm.SupserstoreOrder.discount)
                    .where(mm.SupserstoreOrder.id.in_(subq_id))
                    .subquery()
    )
    stmt = sa.select(mm.Category.id.label('category_id'), 
                     subq_products.c.product_no, 
                     subq_products.c.product_name, 
                     subq_products.c.sales, 
                     subq_products.c.quantity, 
                     subq_products.c.discount).join_from(
        subq_products, mm.Category, mm.Category.name == subq_products.c.sub_cate
    )
    # print(stmt)
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Product), [
                {'product_no': product_no,
                 'category_id': category_id,
                'name': product_name,
                'price': unit_price(sales, quantity, discount)}
                for category_id, product_no, product_name, sales, quantity, discount
                    in session.execute(stmt)
            ],
        )
        session.commit()

# etl orders table
def etl_orders():
    subq = (sa.select(mm.SupserstoreOrder.customer_no, mm.SupserstoreOrder.order_no,
                      mm.SupserstoreOrder.order_at, mm.SupserstoreOrder.return_status_id)
                      .group_by(mm.SupserstoreOrder.customer_no, mm.SupserstoreOrder.order_no,
                      mm.SupserstoreOrder.order_at, mm.SupserstoreOrder.return_status_id)
                    .subquery()
    )
    stmt = sa.select(mm.Customer.id.label('customer_id'), subq.c.order_no,
                      subq.c.order_at, subq.c.return_status_id).join_from(
        subq, mm.Customer, subq.c.customer_no == mm.Customer.customer_no
    )
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Order), [
                {'order_no': order_no, 'customer_id': customer_id,
                'status_id': 1 if status_id == 1 else 2,
                'order_date': order_date}
                for customer_id, order_no, order_date, status_id in session.execute(stmt)
            ],
        )
        session.commit()

# ETL product_order table  
def etl_product_order():
    subq = (sa.select(mm.SupserstoreOrder.order_no, mm.SupserstoreOrder.product_no,
                      sa.func.sum(mm.SupserstoreOrder.sales).label('sum_sales'),
                      sa.func.sum(mm.SupserstoreOrder.quantity).label('sum_quantity'))
                    .group_by(mm.SupserstoreOrder.order_no, mm.SupserstoreOrder.product_no)
                    .subquery()
    )
    stmt = sa.select(subq.c.sum_sales, subq.c.sum_quantity, 
                     mm.Product.price, mm.Product.id.label('product_id'), 
                     mm.Order.id.label('order_id')).join(
        mm.Product, mm.Product.product_no == subq.c.product_no
    ).join(
        mm.Order, mm.Order.order_no == subq.c.order_no
    )
    # print(stmt)
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.ProductOrder),[
                {'quantity': sum_quantity, 'order_price': price,
                 'order_discount': round(1-sum_sales/(sum_quantity*price), 2), 
                 'order_id': order_id,
                 'product_id': product_id}
                for sum_sales, sum_quantity, price, product_id, order_id
                    in session.execute(stmt)
            ]

        )
        session.commit()

# etl shipment data
def etl_shipment():
    subq = (sa.select(mm.SupserstoreOrder.order_no, mm.SupserstoreOrder.ship_mode,
                      mm.SupserstoreOrder.ship_at)
                    .subquery()
    )
    stmt = sa.select(subq.c.ship_mode, subq.c.ship_at, mm.Order.id.label('order_id')).join_from(
        subq, mm.Order, subq.c.order_no == mm.Order.order_no
    )
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(mm.Shipment), [
                {'order_id':order_id,
                 'ship_mode': ship_mode,
                 'ship_date': ship_at}
                for ship_mode, ship_at, order_id in session.execute(stmt)
            ]
        )
        session.commit()

from datetime import datetime
from decimal import Decimal
from typing import List
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Numeric
# from sqlalchemy import Integer
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy import String
from sqlalchemy import Index
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

mapper_registry = registry()

class Base(DeclarativeBase):
    pass

# metadata table
class Metadata(Base):
    __tablename__ = 'metadatas'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(50), nullable=False, comment='table name')
    column_name: Mapped[Optional[str]] = mapped_column(String(100), comment='column name')
    data_type: Mapped[Optional[str]] = mapped_column(String(50), comment='data type for the column, eg: int')
    description: Mapped[Optional[str]] = mapped_column(String(255), comment='description')
    constraints: Mapped[Optional[str]] = mapped_column(String(255), comment='constrains, eg: foreign key, index, unique')
    relationships: Mapped[Optional[str]] = mapped_column(String(255), comment='relationships, eg: has many... belongs to...')

    created_at: Mapped[datetime] = mapped_column(insert_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(insert_default=func.now())

    __table_args__ = (
        Index('table_column_name_index', 'table_name', 'column_name', unique=True),
    )
    
    def __repr__(self):
        return f'<Metadata {self.id} ({self.table_name} - {self.column_name})>'

# reporting data
class ExecutiveReport(Base):
    __tablename__ = 'executive_reports'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)

# reporting data
class OperationalReport(Base):
    __tablename__ = 'operational_reports'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)


# reference data
class Segment(Base):
    __tablename__ = 'segments'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment='segment name, such as "Home Office"')

    # one segment can be assignment to multiple customers
    customers:Mapped[List['Customer']] = relationship(back_populates='segment')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'

# master data
class Customer(Base):
    __tablename__ = 'customers'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    customer_no: Mapped[Optional[str]] = mapped_column(String(15), nullable=False, comment='generated customer number, such as "ABC-SDF-121123"')
    first_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='first name')
    mid_name: Mapped[Optional[str]] = mapped_column(String(30), comment='middle name')
    last_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='last name')

    segment_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('segments.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to segments table')
    # one customer has one segment
    segment:Mapped['Segment'] = relationship(back_populates='customers')
    # one customer have many orders
    orders:Mapped[List['Order']] = relationship(back_populates='customer')
    # customer have mupltiple addresses
    address_customers:Mapped[List['AddressCustomer']] = relationship(back_populates='customer')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.customer_no} references to segment: {self.segment_id})>'

# transaction data
class OrderStatus(Base):
    __tablename__ = 'order_statuses'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(30), nullable=False, comment='order status label, such as "returned"')

    # one order status label can be assigned to multiple orders
    orders:Mapped[List['Order']] = relationship(back_populates='order_status')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'

# transaction data
class Order(Base):
    __tablename__ = 'orders'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    order_no: Mapped[str] = mapped_column(String(50), nullable=False, comment='generated order number, such as "ABC-SDF-121123"')
    order_date: Mapped[datetime] = mapped_column(insert_default=func.now())
    customer_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('customers.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to customers table')
    
    status_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('order_statuses.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to order_statuses table')
    # one order belongs to one customer
    customer:Mapped['Customer'] = relationship(back_populates='orders')
    # one order has one order status label
    order_status:Mapped['OrderStatus'] = relationship(back_populates='orders')
    # one order has one shipment info
    shipment:Mapped['Shipment'] = relationship(back_populates='order')
    # one order has many product orders
    product_orders:Mapped[List['ProductOrder']] = relationship(back_populates='order')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.order_no}, {self.customer_id})>'
    
# transaction data
class Shipment(Base):
    __tablename__ = 'shipments'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    ship_mode: Mapped[str] = mapped_column(String(50), nullable=False, comment='ship mode, such as ECONOMIC                                                                   ')
    ship_date: Mapped[datetime] = mapped_column(insert_default=func.now())
    order_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('orders.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to orders table')
    # one shippment info belongs to one order
    order:Mapped['Order'] = relationship(back_populates='shipment')
    # order_status:Mapped['OrderStatus'] = relationship(back_populates='orders')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.ship_date} {self.order_id})>'
    
# master data
class Employee(Base):
    __tablename__ = 'employees'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='first name')
    mid_name: Mapped[Optional[str]] = mapped_column(String(30), comment='middle name')
    last_name: Mapped[str] = mapped_column(String(30), nullable=False, comment='last name')

    region_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('regions.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to regions table')
    # one employee has one region
    region:Mapped['Region'] = relationship(back_populates='employee')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.first_name} references to segment: {self.region_id})>'

# reference data
class Region(Base):
    __tablename__ = 'regions'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='region name, such as "West"')

    # one region belongs to one employee
    employee:Mapped['Employee'] = relationship(back_populates='region')
    # one region has many states
    states:Mapped[List['State']] = relationship(back_populates='region')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'
    
# reference data
class Country(Base):
    __tablename__ = 'countries'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='country name, such as "Canada"')
    code_2: Mapped[Optional[str]] = mapped_column(String(2), comment='country 2-alpha code, such as "CA"')
    code_3: Mapped[Optional[str]] = mapped_column(String(3), comment='country 3-alpha code, such as "CAN"')

    # one country has many states
    states:Mapped[List['State']] = relationship(back_populates='country')
    

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'
    
# reference data
class State(Base):
    __tablename__ = 'states'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='state name, such as "Ontario"')

    country_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('countries.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to countries table')
    region_id: Mapped[Optional[INTEGER(unsigned=True)]] = mapped_column(ForeignKey('regions.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to regions table')
    # one state belongs to one region
    region:Mapped['Region'] = relationship(back_populates='states')
    # one state belongs to one country
    country:Mapped['Country'] = relationship(back_populates='states')
    # one state has many cities
    cities:Mapped[List['City']] = relationship(back_populates='state')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'
    
# reference data
class City(Base):
    __tablename__ = 'cities'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='city name, such as "Toronto"')

    state_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('states.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to states table')

    # one city belongs to one state
    state:Mapped['State'] = relationship(back_populates='cities')
    # one city has many addresses
    addresses:Mapped[List['Address']] = relationship(back_populates='city')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'
    
# reference data
class Address(Base):
    __tablename__ = 'addresses'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(100), nullable=False, comment='address name, such as "52 fairglen ave"')
    post_code: Mapped[str] = mapped_column(String(15), nullable=False, comment='post code, such as "123123"')

    city_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('cities.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to cities table')

    # one address belongs one city
    city:Mapped['City'] = relationship(back_populates='addresses')
    # one address has many customer addresses
    address_customers:Mapped[List['AddressCustomer']] = relationship(back_populates='address')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.post_code})>'
    
# master data
class AddressCustomer(Base):
    __tablename__ = 'address_customers'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    customer_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('customers.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to customers table')
    address_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('addresses.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to addresses table')
    __table_args__ = (
        Index('ck_customerid_addressid', 'customer_id', 'address_id', unique=True), 
    )

    # one address customer reflects one address info
    address:Mapped['Address'] = relationship(back_populates='address_customers')
    # one address customer reflects one customer info
    customer:Mapped['Customer'] = relationship(back_populates='address_customers')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.customer_id})>'
    
# reference data
class Category(Base):
    __tablename__ = 'categories'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment='category name, such as "Fruit"')

    parent_id: Mapped[Optional[INTEGER(unsigned=True)]] = mapped_column(ForeignKey('categories.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: self-references to categories table')

    # one sub-category belongs to one category
    parent:Mapped[Optional['Category']] = relationship(back_populates='children')
    # one category has many sub-category
    children:Mapped[Optional[List['Category']]] = relationship(back_populates='parent', remote_side='Category.id')
    # category can be assignment to many products
    products:Mapped[List['Product']] = relationship(back_populates='category')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name})>'
    
# master data
class Product(Base):
    __tablename__ = 'products'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    product_no: Mapped[str] = mapped_column(String(15), nullable=False, comment='generated product number, such as "ABC-SDF-121123"')
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment='product name, such as "Computer"')
    price: Mapped[int] = mapped_column(Numeric(12, 2), nullable=False, comment='product price, such as "1233.23"')
    discount: Mapped[int] = mapped_column(Numeric(4, 2), nullable=False, comment='product discount, such as "0.23"')
    category_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('categories.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to categories table')
    
    # one product belongs to one category
    category:Mapped['Category'] = relationship(back_populates='products')
    # one product belongs to many orders
    # orders:Mapped[List['Order']] = relationship(back_populates='product')
    # one product has many product orders
    product_orders:Mapped[List['ProductOrder']] = relationship(back_populates='product')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.name}, {self.category_id})>'
    
# master data
class ProductOrder(Base):
    __tablename__ = 'product_orders'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    quantity: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    order_price: Mapped[int] = mapped_column(Numeric(12, 2), nullable=False, comment='product price in order, such as "1233.23"')
    order_discount: Mapped[int] = mapped_column(Numeric(4, 2), nullable=False, comment='product discount in order, such as "0.23"')

    product_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('products.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to products table')
    order_id: Mapped[INTEGER(unsigned=True)] = mapped_column(ForeignKey('orders.id', ondelete='NO ACTION', onupdate='CASCADE'), 
                                                               nullable=False, 
                                                               comment='fk: references to orders table')
    
    __table_args__ = (
        Index('ck_productid_orderid', 'product_id', 'order_id', unique=True), 
    )

    # one product order reflects many products
    product:Mapped['Product'] = relationship(back_populates='product_orders')
    # one product order reflects many orders
    order:Mapped['Order'] = relationship(back_populates='product_orders')

    def __repr__(self):
        return f'<Metadata {self.id} - {self.order_id}, {self.product_id})>'
    

# master data
class SupserstoreOrder(Base):
    __tablename__ = 'superstore_orders'

    id = mapped_column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    row_id: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    product_no: Mapped[str] = mapped_column(String(15), nullable=False, comment='generated product number, such as "ABC-SDF-121123"')
    order_no: Mapped[str] = mapped_column(String(50), nullable=False, comment='generated order number, such as "ABC-SDF-121123"')
    ship_mode: Mapped[Optional[str]] = mapped_column(String(255))
    customer_no: Mapped[Optional[str]] = mapped_column(String(255))
    customer_name: Mapped[Optional[str]] = mapped_column(String(255))
    segment: Mapped[Optional[str]] = mapped_column(String(255))
    country: Mapped[Optional[str]] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(255))
    state: Mapped[Optional[str]] = mapped_column(String(255))
    post_code: Mapped[Optional[str]] = mapped_column(String(255))
    region: Mapped[Optional[str]] = mapped_column(String(255))
    category: Mapped[Optional[str]] = mapped_column(String(255))
    sub_cate: Mapped[Optional[str]] = mapped_column(String(255))
    product_name: Mapped[Optional[str]] = mapped_column(String(255))
    sales: Mapped[Optional[int]] = mapped_column(Numeric(12, 2))
    discount: Mapped[Optional[int]] = mapped_column(Numeric(4, 2))
    quantity: Mapped[int] = mapped_column(INTEGER(unsigned=True))
    profit: Mapped[int] = mapped_column(Numeric(12, 2), nullable=False, comment='product discount in order, such as "0.23"')

    order_at: Mapped[Optional[datetime]]
    ship_at: Mapped[Optional[datetime]]

    def __repr__(self):
        return f'<Metadata {self.id} - {self.order_no}, {self.product_name})>'
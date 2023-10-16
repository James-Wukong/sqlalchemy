import csv
import decimal
import os
from sqlalchemy.orm import Session, aliased
import sqlalchemy as sa

from src.constants import ROOT_DIR
from src.database import engine
from src.models import mapped_models as mm

engine = engine.sql_engine()
er_filename = os.path.join(ROOT_DIR, 'data', 'executive_report.csv')
opr_filename = os.path.join(ROOT_DIR, 'data', 'operational_report.csv')

class ReportBase():
    engine = None

    def __init__(self, engine):
        self.engine = engine

    
    def gen_report(self, data=[], headers=[], csv_file=''):
        # writing to csv file  
        with open(csv_file, 'w') as csvfile:  
            # creating a csv writer object  
            csvwriter = csv.writer(csvfile)  
            csvwriter.writerow(headers)  
            # writing the data rows
            csvwriter.writerows(data)

class ExecutiveReport(ReportBase):
    '''
    generate executive report
    '''
    def query_orders(self, session, start_date, end_date):
        return (session.query(sa.extract('year', mm.Order.order_date).label('year'), 
                            mm.Region.name.label('region'),
                            sa.func.sum(mm.ProductOrder.order_price * mm.ProductOrder.quantity * (1 - mm.ProductOrder.order_discount)).label('order_sales'),
                            sa.func.sum(mm.ProductOrder.order_profit).label('order_profits'),
                            sa.func.sum(mm.ProductOrder.order_price * mm.ProductOrder.quantity * (1 - mm.ProductOrder.order_discount) - mm.ProductOrder.order_profit).label('order_cogs'),
                            )
                .select_from(mm.ProductOrder)
                .join(mm.Order, mm.ProductOrder.order_id == mm.Order.id)
                .join(mm.Customer, mm.Customer.id == mm.Order.customer_id)
                
                # .join(mm.AddressCustomer, mm.Customer.id == mm.AddressCustomer.customer_id)
                .join(mm.Address, mm.Order.address_id == mm.Address.id)
                .join(mm.City, mm.Address.city_id == mm.City.id)
                .join(mm.State, mm.City.state_id == mm.State.id)
                .join(mm.Region, mm.Region.id == mm.State.region_id)
                # .join(mm.Product, mm.Product.id == mm.ProductOrder.product_id, isouter=True)
                .filter(mm.Order.order_date.between(start_date, end_date))
                .group_by(sa.extract('year', mm.Order.order_date),
                            mm.Region.name, 
                            )
                .order_by('year', 'region')
                .all())



class OperationalReport(ReportBase):
    '''
    generate operational report
    '''
    def query_orders(self, session, start_date, end_date):
        category = aliased(mm.Category)
        category_p = aliased(mm.Category)
        return (session.query(
                              sa.func.date_format(mm.Order.order_date, '%Y-%m').label('year_and_month'),
                            #   mm.ProductOrder,
                            mm.Region.name.label('region'),
                            mm.State.name.label('state'),
                            mm.City.name.label('city'),
                            category_p.name.label('category'),
                            sa.func.count(mm.Order.id).label('sum_orders'),
                            sa.func.sum(sa.case (
                                (mm.Order.status_id == 2, 1),
                                else_=0
                            ) ).label('sum_returned'),
                            sa.func.sum(mm.ProductOrder.order_price * mm.ProductOrder.quantity * (1 - mm.ProductOrder.order_discount)).label('sum_sales'),
                            sa.func.sum(mm.ProductOrder.order_profit).label('sum_profits'),
                            sa.func.sum(mm.ProductOrder.order_price * mm.ProductOrder.quantity * (1 - mm.ProductOrder.order_discount) - mm.ProductOrder.order_profit).label('sum_cogs'),
                            )
                .select_from(mm.ProductOrder)
                .join(mm.Order, mm.ProductOrder.order_id == mm.Order.id)
                .join(mm.Product, mm.ProductOrder.product_id == mm.Product.id)
                .join(category, category.id == mm.Product.category_id)
                .join(category_p, category_p.id == category.parent_id)
                .join(mm.Customer, mm.Customer.id == mm.Order.customer_id)
                .join(mm.Address, mm.Order.address_id == mm.Address.id)
                .join(mm.City, mm.Address.city_id == mm.City.id)
                .join(mm.State, mm.City.state_id == mm.State.id)
                .join(mm.Region, mm.Region.id == mm.State.region_id)
                
                # .join(mm.Product, mm.Product.id == mm.ProductOrder.product_id, isouter=True)
                .filter(mm.Order.order_date.between(start_date, end_date))
                .group_by(sa.func.date_format(mm.Order.order_date, '%Y-%m'),
                            mm.Region.name, 
                            mm.State.name,
                            mm.City.name,
                            category_p.name,
                            )
                .order_by('year_and_month', 'region', 'state', 'city', 'category')
                .all()
        )

if __name__ == '__main__':
    kpi_performance = 0.1 # 10% increase of sales yearly
    er = ExecutiveReport(engine)
    er_data = []
    er_headers = ['Region', 'Sales 2020 ($)', 'Sales 2021 ($)', 'Sales 2021 vs 2020 (%)', 'KPI Performance', 
               'Profit 2021 ($)', 'Profit 2021 vs 2020 (%)', 
               'COGS 2020 (%)', 'COGS 2021 (%)']
    with Session(bind=engine) as session:
        q_2020 = er.query_orders(session=session, start_date='2020-01-01', end_date='2020-12-31')
        q_2021 = er.query_orders(session=session, start_date='2021-01-01', end_date='2021-12-31')
    for i in range(len(q_2021)):
        er_data.append((q_2021[i][1],
                     q_2020[i][2],
                     q_2021[i][2],
                     f'{round(((q_2021[i][2] - q_2020[i][2])/q_2020[i][2]) * 100, 2)}%',
                     'Above Target' if q_2021[i][2] >= (q_2020[i][2] * (1 + decimal.Decimal(kpi_performance))) else 'Below Target',
                     q_2021[i][3],
                     f'{round(((q_2021[i][3] - q_2020[i][3])/q_2020[i][3]) * 100, 2)}%',
                    #  q_2021[i][2] - q_2021[i][3],
                    #  f'{round(((q_2021[i][2] - q_2021[i][3] - (q_2020[i][2] - q_2020[i][3]))/(q_2020[i][2] - q_2020[i][3])) * 100, 2)}%',
                    f'{round(((q_2020[i][2] - q_2020[i][3])/q_2020[i][2]) * 100, 2)}%',
                    f'{round(((q_2021[i][2] - q_2021[i][3])/q_2021[i][2]) * 100, 2)}%',
                     ))
    # generate executive report
    er.gen_report(er_data, er_headers, er_filename)

    opr = OperationalReport(engine)
    opr_headers = ['Period', 'Region', 'State', 'City', 'Category', 
               'Total Orders', 'Total Returns', 
               'Total Sales', 'Total Profits',
               'Total COGS']
    with Session(bind=engine) as session:
        q = opr.query_orders(session=session, start_date='2021-01-01', end_date='2021-03-31')
    # generate operational report
    opr.gen_report(q, opr_headers, opr_filename)
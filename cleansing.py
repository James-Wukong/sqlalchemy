import sqlalchemy as sa
import pandas as pd
import os
from sqlalchemy.orm import Session
from src.models.mapped_models import SupserstoreOrder
from src.database import engine as db_engine
from src.constants import ROOT_DIR

data_file = os.path.join(ROOT_DIR, 'data', 'Sample - Superstore.xls')

df_orders = pd.read_excel(data_file, sheet_name='Orders')
df_people = pd.read_excel(data_file, sheet_name='People')
df_returns = pd.read_excel(data_file, sheet_name='Returns')

engine = db_engine.sql_engine()

# update the product name so that it's aligned with product no
# doing this by updating every row of product name with the latest name used
def clean_products_v1():
    stmt_sets = (
        sa.select(SupserstoreOrder.product_no, SupserstoreOrder.product_name, sa.func.min(SupserstoreOrder.id))
        .group_by(SupserstoreOrder.product_no, SupserstoreOrder.product_name)
        .order_by(SupserstoreOrder.product_no)
    )
    with Session(bind=engine) as session:
        for p_no, p_name, _ in session.execute(stmt_sets):
            update_stmt = (
                sa.update(SupserstoreOrder)
                .where(SupserstoreOrder.product_no == p_no)
                .values(
                    {'product_name': p_name}
                )
            )
            session.execute(update_stmt)
            session.commit()

# add return status id to superstore orders table
def fillin_order_status():
    df_status = df_returns.drop_duplicates()

    with Session(bind=engine) as session:
        for i in range(len(df_status)):
            update_stmt = (
                sa.update(SupserstoreOrder)
                .where(SupserstoreOrder.order_no == df_status.iloc[i, 1])
                .values(
                    {'return_status_id': 1 if df_status.iloc[i, 0].capitalize() == 'Yes' else 2}
                )
            )
            session.execute(update_stmt)
            session.commit()

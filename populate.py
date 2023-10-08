import os
import pandas as pd
from sqlalchemy.orm import Session
import sqlalchemy as sa

from src.constants import ROOT_DIR
from src.database import engine as db_engine
from src.models.mapped_models import Metadata

data_file = os.path.join(ROOT_DIR, 'data', 'Sample - Superstore.xls')
engine = db_engine.sql_engine()

# df_orders = pd.read_excel(data_file, sheet_name='Orders')
# df_people = pd.read_excel(data_file, sheet_name='People')
# df_returns = pd.read_excel(data_file, sheet_name='Returns')

# country = df_orders['Segment'].unique()


def insert_metadatas():
    with Session(bind=engine) as session:
        session.execute(
            sa.insert(Metadata), [
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

insert_metadatas()
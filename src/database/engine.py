import sqlalchemy as sa
import toml
from src.constants import ROOT_DIR

def sql_engine():
    config = toml.load(f'{ROOT_DIR}/settings/config.toml')
    engine = sa.create_engine(f"mysql+pymysql://{config['database']['user']}:{config['database']['password']}@{config['database']['host']}/{config['database']['db_name']}", \
                                echo=True, pool_recycle=3600)
    return engine
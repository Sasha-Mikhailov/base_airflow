from sqlalchemy.dialects.postgresql import VARCHAR, NUMERIC, DATE, TIMESTAMP
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = MetaData()


rates = Table(
    'exchange_rates', Base.metadata,
    Column('currency_from', VARCHAR()),
    Column('currency_to', VARCHAR()),
    Column('date', DATE()),
    Column('rate', NUMERIC(15, 6)),
    Column('utc_created_dttm', TIMESTAMP()),
    Column('utc_updated_dttm', TIMESTAMP()),
)


def create_if_not_exists(engine, table_name):
    if engine.dialect.has_table(engine, table_name):
        return

    metadata = MetaData(engine)
    rates.metadata = metadata
    metadata.create_all()

from sqlalchemy.dialects.postgresql import VARCHAR, NUMERIC, DATE, TIMESTAMP
from sqlalchemy import Table, Column, MetaData, Numeric, Date, String, DateTime
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = MetaData()


rates = Table(
    'exchange_rates', Base.metadata,
    Column('currency_from', String()),
    Column('currency_to', String()),
    Column('date', Date()),
    Column('rate', Numeric(15, 6)),
    Column('utc_created_dttm', DateTime),
    Column('utc_updated_dttm', DateTime),
)


def create_table_if_not_exists(engine, table_name):
    if engine.dialect.has_table(engine, table_name):
        return

    metadata = MetaData(engine)
    rates.metadata = metadata
    metadata.create_all()

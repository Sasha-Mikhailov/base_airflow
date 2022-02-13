from datetime import datetime

from sqlalchemy import Table, Column, MetaData, Numeric, Date, String, DateTime
from sqlalchemy.engine import Engine


rates = Table(
    'exchange_rates', MetaData(),
    Column('currency_from', String()),
    Column('currency_to', String()),
    Column('date', Date(), index=True),  # create index to optimise idempotency deletions
    Column('rate', Numeric(15, 6)),
    Column('utc_created_dttm', DateTime),
    Column('utc_updated_dttm', DateTime, default=datetime.utcnow()),
)


def create_table_if_not_exists(engine: Engine, table: Table):
    with engine.connect() as conn:
        if engine.dialect.has_table(conn, table.name):
            print(f'Table {table.name} exists!')
            return

        table.metadata.create_all(conn)
        print(f'Table {table.name} created')

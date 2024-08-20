import pendulum
from airflow.decorators import dag, task


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
)
def prepare_churn_dataset():
    """
    Prepares the churn dataset by extracting data from multiple tables,
    transforming the data, and loading it into a destination database table.
    """
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table(**kwargs) -> None:
        """
        Creates a SQLAlchemy table named "users_churn" with the specified
        columns, if the table does not already exist in the destination
        database.

        The table also has a unique constraint on the "customer_id" column.
        """
        from sqlalchemy import (
            inspect,
            MetaData,
            Table,
            Column,
            String,
            Integer,
            DateTime,
            Float,
            UniqueConstraint,
        )

        hook = PostgresHook("destination_db")
        conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        table = Table(
            "users_churn",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("customer_id", String),
            Column("begin_date", DateTime),
            Column("end_date", DateTime),
            Column("type", String),
            Column("paperless_billing", String),
            Column("payment_method", String),
            Column("monthly_charges", Float),
            Column("total_charges", Float),
            Column("internet_service", String),
            Column("online_security", String),
            Column("online_backup", String),
            Column("device_protection", String),
            Column("tech_support", String),
            Column("streaming_tv", String),
            Column("streaming_movies", String),
            Column("gender", String),
            Column("senior_citizen", Integer),
            Column("partner", String),
            Column("dependents", String),
            Column("multiple_lines", String),
            Column("target", Integer),
            UniqueConstraint(
                "customer_id", name="unique_customer_id_constraint"
            ),
        )

        if not inspect(conn).has_table(table.name):
            metadata.create_all(conn)

    @task()
    def extract(**kwargs) -> pd.DataFrame:
        """
        Extracts data from multiple tables in the source database and
        returns a pandas DataFrame containing the extracted data.

        The data is extracted by performing a left join across the
        contracts, internet, personal, and phone tables in the source
        database.

        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the extracted data.
        """
        hook = PostgresHook("source_db")
        conn = hook.get_conn()
        sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, 
            c.paperless_billing, c.payment_method, c.monthly_charges, 
            c.total_charges, i.internet_service, i.online_security, 
            i.online_backup, i.device_protection, i.tech_support, 
            i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input data by:
        - Creating a new "target" column that indicates whether the
        customer churned (1) or not (0) based on the "end_date" column.
        - Replacing any "No" values in the "end_date" column with None.

        Args:
            data (pd.DataFrame):
                A pandas DataFrame containing the extracted data.

        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the transformed data.
        """
        data["target"] = (data["end_date"] != "No").astype(int)
        data["end_date"].replace({"No": None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        """
        Loads the transformed data into the "users_churn"
        table in the destination database.

        Args:
            data (pd.DataFrame):
                A pandas DataFrame containing the transformed
                data to be loaded.
        """
        hook = PostgresHook("destination_db")
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["customer_id"],
            rows=data.values.tolist(),
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


prepare_churn_dataset()

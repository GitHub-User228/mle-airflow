import pendulum
from airflow.decorators import dag, task

from steps.clean_churn import (
    fill_missing_values,
    iqr_filter,
    remove_duplicates,
)


@dag(
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL"],
)
def clean_churn_dataset():
    """
    Cleans the churn dataset by performing the specified tasks.
    """
    import numpy as np
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table(**kwargs) -> None:
        """
        Creates a table named "clean_users_churn" in the destination
        database with the specified columns:

        The table has a unique constraint on the customer_id column.
        If the table does not already exist, it will be created in the
        destination database.
        """
        from sqlalchemy import (
            Table,
            Column,
            DateTime,
            Float,
            Integer,
            MetaData,
            String,
            UniqueConstraint,
            inspect,
        )

        hook = PostgresHook("destination_db")
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        churn_table = Table(
            "clean_users_churn",
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
                "customer_id", name="unique_clean_customer_constraint"
            ),
        )
        if not inspect(db_engine).has_table(churn_table.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        """
        Extracts data from the "users_churn" table in the destination
        database and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame:
                A pandas DataFrame containing the extracted data,
                with the "id" column removed.
        """
        hook = PostgresHook("destination_db")
        conn = hook.get_conn()
        sql = "SELECT * FROM users_churn"
        data = pd.read_sql(sql, conn).drop(columns=["id"])
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame) -> pd.DataFrame:
        data = remove_duplicates(data)
        data = iqr_filter(data)
        data = fill_missing_values(data)
        return data

    @task()
    def load(data: pd.DataFrame) -> None:
        """
        Loads the transformed data into the "clean_users_churn"
        table in the destination database.

        Args:
            data (pd.DataFrame):
            The transformed data to be loaded.
        """
        hook = PostgresHook("destination_db")
        data["end_date"] = (
            data["end_date"].astype("object").replace(np.nan, None)
        )
        hook.insert_rows(
            table="clean_users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=["customer_id"],
            rows=data.values.tolist(),
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


clean_churn_dataset()

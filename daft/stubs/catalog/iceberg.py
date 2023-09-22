# mypy: ignore-errors

from __future__ import annotations

from contextlib import AbstractContextManager

import daft
from daft.daft import PartitionScheme, PartitionSpec
from daft.dataframe import DataFrame
from daft.logical.schema import Field, Schema


class IcebergCatalog:
    # creator functions
    @classmethod
    def from_glue(cls, args) -> IcebergCatalog:
        ...

    def from_storage(cls, args) -> IcebergCatalog:
        ...

    # namespace functions
    def list_namespaces(self) -> DataFrame:
        ...

    def create_namespace(self, namespace: str) -> bool:
        ...

    def drop_namespace(self, namespace: str) -> bool:
        ...

    # table functions
    def list_tables(self) -> DataFrame:
        ...

    def create_table(
        self,
        identifier: str,
        schema: Schema,
        partition_spec: PartitionSpec | None = None,
        sort_columns: list[str] | None = None,
    ) -> Table:
        ...

    def load_table(self, identifier: str) -> Table:
        ...


class Table:
    def history(self) -> DataFrame:
        ...

    def schema(self) -> Schema:
        ...

    def schemas(self) -> dict[int, Schema]:
        ...

    def partition_spec(self) -> PartitionSpec:
        ...

    def partition_specs(self) -> dict[int, PartitionSpec]:
        ...

    def update_schema(self) -> SchemaUpdate:
        ...

    def update_partition_spec(self) -> PartitionSpecUpdate:
        ...


class SchemaUpdate(AbstractContextManager):
    def add_column(self, field: Field) -> SchemaUpdate:
        ...

    def drop_column(self, name: str) -> SchemaUpdate:
        ...

    def rename_column(self, name: str, new_name: str) -> SchemaUpdate:
        ...


class PartitionSpecUpdate(AbstractContextManager):
    def add_partitioning_field(self, name: str, scheme: PartitionScheme) -> PartitionSpecUpdate:
        ...


class DataframeIcebergNamespace:
    def __init__(self, df: DataFrame) -> None:
        self.df = df

    @classmethod
    def read(cls, catalog: IcebergCatalog, table_identifier: str, snapshot_id: int | None = None) -> DataFrame:
        """Produces a lazy daft DataFrame that is backed by an Iceberg table.

        Args:
            catalog (IcebergCatalog): Iceberg catalog to read from
            table_identifier (str): table name to read from
            snapshot_id (Optional[int], optional): snapshot id of table to read from. Defaults to None, which is the latest snapshot.

        Returns:
            DataFrame: a lazy daft dataframe that is backed by the input iceberg table.
        """
        ...

    def append(self, catalog: IcebergCatalog, table_identifier: str) -> None:
        """Appends records from a daft DataFrame into an Iceberg table following it's Partitioning Spec.
        This operation will not affect any of the existing records in the Iceberg Table.

        Args:
            catalog (IcebergCatalog): Iceberg catalog to write to
            table_identifier (str): table name to write to
        """
        ...

    def overwrite(self, catalog: IcebergCatalog, table_identifier: str) -> None:
        """Overwrites the records in this Iceberg Table from a daft DataFrame.
        This operation follows the Iceberg Table's Schema, Partitioning Scheme and properties when writing the new records.

        Args:
            catalog (IcebergCatalog): Iceberg catalog to write to
            table_identifier (str): table name to write to
        """
        ...


def example_deleting_rows() -> None:
    catalog = IcebergCatalog.from_glue("path/to/glue")
    df: DataFrame = daft.read_iceberg(catalog, "my-table")
    df = df.where(df["id"] > 10 & df["id"] < 20)
    df.iceberg.overwrite(catalog, "my-table")


def example_updating_rows() -> None:
    catalog = IcebergCatalog.from_glue("path/to/glue")
    df: DataFrame = daft.read_iceberg(catalog, "my-table")
    df = df.with_column("x", (df["x"] < 10).if_else(0, df["x"]))
    df.iceberg.overwrite(catalog, "my-table")

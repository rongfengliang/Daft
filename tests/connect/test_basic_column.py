from __future__ import annotations

from pyspark.sql.functions import col
from pyspark.sql.types import StringType


def test_column_operations(spark_session):
    # Create DataFrame from range(10)
    df = spark_session.range(10)

    # Test __getattr__
    # todo: https://github.com/Eventual-Inc/Daft/issues/3433
    # df_attr = df.select(col("id").desc())  # Fix: call desc() as method
    # assert df_attr.toPandas()["id"].iloc[0] == 9, "desc should sort in descending order"

    # Test __getitem__
    # todo: add extract value
    # df_item = df.select(col("id")[0])
    # assert df_item.toPandas()["id"].iloc[0] == 0, "getitem should return first element"

    # Test alias
    df_alias = df.select(col("id").alias("my_number"))
    assert "my_number" in df_alias.columns, "alias should rename column"
    assert df_alias.toPandas()["my_number"].equals(df.toPandas()["id"]), "data should be unchanged"

    # Test cast
    df_cast = df.select(col("id").cast(StringType()))
    assert df_cast.schema.fields[0].dataType == StringType(), "cast should change data type"

    # Test isNotNull/isNull
    df_null = df.select(col("id").isNotNull().alias("not_null"), col("id").isNull().alias("is_null"))
    assert df_null.toPandas()["not_null"].iloc[0], "isNotNull should be True for non-null values"
    assert not df_null.toPandas()["is_null"].iloc[0], "isNull should be False for non-null values"

    # Test name
    df_name = df.select(col("id").name("renamed_id"))
    assert "renamed_id" in df_name.columns, "name should rename column"
    assert df_name.toPandas()["renamed_id"].equals(df.toPandas()["id"]), "data should be unchanged"



    # Test asc/desc todo: sort
    # df_asc = df.select(col("id").asc())
    # assert df_asc.toPandas()["id"].iloc[0] == 0, "asc should sort in ascending order"

    # df_asc_nulls = df.select(col("id").asc_nulls_first())
    # assert df_asc_nulls.toPandas()["id"].iloc[0] == 0, "asc_nulls_first should sort ascending with nulls first"

    # Test astype (alias for cast)
    df_astype = df.select(col("id").astype(StringType()))
    assert df_astype.schema.fields[0].dataType == StringType(), "astype should change data type"

    # Test between
    df_between = df.select(col("id").between(3, 6).alias("in_range"))
    assert df_between.toPandas()["in_range"].tolist() == [False, False, False, True, True, True, True, False, False, False]

    # Test string operations
    # df_str = spark_session.createDataFrame([("hello",), ("world",)], ["text"])

    # df_contains = df_str.select(col("text").contains("o").alias("has_o"))
    # assert df_contains.toPandas()["has_o"].tolist() == [True, True]

    # df_startswith = df_str.select(col("text").startswith("h").alias("starts_h"))
    # assert df_startswith.toPandas()["starts_h"].tolist() == [True, False]

    # df_endswith = df_str.select(col("text").endswith("d").alias("ends_d"))
    # assert df_endswith.toPandas()["ends_d"].tolist() == [False, True]

    # df_substr = df_str.select(col("text").substr(1, 2).alias("first_two"))
    # assert df_substr.toPandas()["first_two"].tolist() == ["he", "wo"]

    # # Test struct operations
    # df_struct = spark_session.createDataFrame([
    #     ({"a": 1, "b": 2},),
    #     ({"a": 3, "b": 4},)
    # ], ["data"])

    # df_getfield = df_struct.select(col("data").getField("a").alias("a_val"))
    # assert df_getfield.toPandas()["a_val"].tolist() == [1, 3]

    # df_dropfields = df_struct.select(col("data").dropFields("a").alias("no_a"))
    # assert "a" not in df_dropfields.toPandas()["no_a"][0]

    # df_withfield = df_struct.select(col("data").withField("c", col("data.a") + 10).alias("with_c"))
    # assert df_withfield.toPandas()["with_c"][0]["c"] == 11

    # # Test array operations
    # df_array = spark_session.createDataFrame([([1, 2, 3],), ([4, 5, 6],)], ["numbers"])

    # df_getitem = df_array.select(col("numbers").getItem(0).alias("first"))
    # assert df_getitem.toPandas()["first"].tolist() == [1, 4]

    # # Test when/otherwise
    # df_case = df.select(
    #     col("id").when(col("id") < 5, "low")
    #             .otherwise("high")
    #             .alias("category")
    # )
    # assert df_case.toPandas()["category"].tolist() == ["low"] * 5 + ["high"] * 5

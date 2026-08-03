"""Reusable PySpark transformations shared across the silver layer."""


class Reusable:
    """Common DataFrame transformations used by every silver table build."""

    @staticmethod
    def drop_columns(df, *columns):
        """Drop one or more columns. Accepts names or a single list."""
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            columns = columns[0]
        return df.drop(*columns)

    @staticmethod
    def dedupe(df, *keys):
        """Drop duplicate rows, optionally on a subset of key columns."""
        if not keys:
            return df.dropDuplicates()
        if len(keys) == 1 and isinstance(keys[0], (list, tuple)):
            keys = keys[0]
        return df.dropDuplicates(list(keys))

    @staticmethod
    def uppercase(df, column):
        """Standardise a string column to uppercase."""
        from pyspark.sql.functions import col, upper
        return df.withColumn(column, upper(col(column)))

    @staticmethod
    def bucket_numeric(df, source_column, target_column, low_max, medium_max):
        """Bucket a numeric column into low / medium / high bands."""
        from pyspark.sql.functions import col, when
        return df.withColumn(
            target_column,
            when(col(source_column) < low_max, "low")
            .when(col(source_column) < medium_max, "medium")
            .otherwise("high"),
        )

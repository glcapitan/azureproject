import dlt

@dlt.table(name="dim_date_stg")
def dim_date_stg():
    return spark.readStream.table("glc_project.silver.dim_date")

dlt.create_streaming_table(name="dim_date")

dlt.create_auto_cdc_flow(
    target="dim_date", source="dim_date_stg",
    keys=["date_key"], sequence_by="date",
    stored_as_scd_type=2
)
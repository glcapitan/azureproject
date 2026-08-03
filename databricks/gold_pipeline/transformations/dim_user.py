import dlt

@dlt.table(name="dim_user_stg")
def dim_user_stg():
    return spark.readStream.table("glc_project.silver.dim_user")

dlt.create_streaming_table(name="dim_user")

dlt.create_auto_cdc_flow(
    target="dim_user",
    source="dim_user_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
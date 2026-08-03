import dlt

@dlt.table(name="fact_stream_stg")
def fact_stream_stg():
    return spark.readStream.table("glc_project.silver.fact_stream")

dlt.create_streaming_table(name="fact_stream")

dlt.create_auto_cdc_flow(
    target="fact_stream", source="fact_stream_stg",
    keys=["stream_id"], sequence_by="stream_timestamp",
    stored_as_scd_type=1
)
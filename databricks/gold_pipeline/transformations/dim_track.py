import dlt

@dlt.table(name="dim_track_stg")
def dim_track_stg():
    return spark.readStream.table("glc_project.silver.dim_track")

dlt.create_streaming_table(name="dim_track")

dlt.create_auto_cdc_flow(
    target="dim_track", source="dim_track_stg",
    keys=["track_id"], sequence_by="updated_at",
    stored_as_scd_type=2
)
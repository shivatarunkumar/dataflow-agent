"""The only side-effecting code in the agent. RETRY_FILE is a create-only copy invalid/ -> landing/.

There is deliberately no delete/move/overwrite code path. ifGenerationMatch=0 makes GCS refuse the copy if
landing/<file> already exists, and the dfa-agent SA has no delete permission anyway.
"""


def retry_file(clients, landing_bucket: str, action: dict, incident_id: str) -> dict:
    params = action["params"]
    if params.get("bucket") != landing_bucket:
        raise PermissionError("RETRY_FILE only operates on the landing bucket")
    if not params["source"].startswith("invalid/") or not params["destination"].startswith("landing/"):
        raise PermissionError("RETRY_FILE copies invalid/... to landing/... only")

    bucket = clients.storage.bucket(landing_bucket)
    source = bucket.get_blob(params["source"])
    if source is None:
        raise FileNotFoundError(f"gs://{landing_bucket}/{params['source']} does not exist")

    destination = bucket.blob(params["destination"])
    destination.content_type = source.content_type
    destination.metadata = {"retry_of_job_id": incident_id, "retried_by": "df-agent",
                            "retry_source": f"gs://{landing_bucket}/{params['source']}"}  # fmt: skip
    token = None
    while True:  # rewrite may need several calls for large objects
        token, _, _ = destination.rewrite(source, token=token, if_generation_match=0)
        if token is None:
            break
    return {"gcs_uri": f"gs://{landing_bucket}/{params['destination']}", "generation": destination.generation,
            "source": f"gs://{landing_bucket}/{params['source']}"}  # fmt: skip

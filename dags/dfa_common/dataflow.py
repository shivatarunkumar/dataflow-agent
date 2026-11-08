"""Read-only Dataflow lookups used by finalize: find a job by name and read its error messages."""

from dfa_common import config


def find_job(job_name: str) -> dict:
    """The newest Dataflow job called job_name ({} if none: e.g. the launch was rejected before a job existed)."""
    from google.cloud import dataflow_v1beta3

    client = dataflow_v1beta3.JobsV1Beta3Client()
    request = dataflow_v1beta3.ListJobsRequest(
        project_id=config.PROJECT_ID, location=config.REGION, filter=dataflow_v1beta3.ListJobsRequest.Filter.ALL,
        page_size=100,
    )  # fmt: skip
    for i, job in enumerate(client.list_jobs(request=request)):  # newest first
        if job.name == job_name:
            return {"id": job.id, "state": job.current_state.name}
        if i >= 300:
            break
    return {}


def error_messages(dataflow_job_id: str, limit: int = 5) -> list:
    """Up to `limit` JOB_MESSAGE_ERROR texts. The TAIL of each is kept: a traceback's real exception is at the end."""
    from google.cloud import dataflow_v1beta3

    client = dataflow_v1beta3.MessagesV1Beta3Client()
    request = dataflow_v1beta3.ListJobMessagesRequest(
        project_id=config.PROJECT_ID, location=config.REGION, job_id=dataflow_job_id,
        minimum_importance=dataflow_v1beta3.JobMessageImportance.JOB_MESSAGE_ERROR, page_size=limit,
    )  # fmt: skip
    messages = []
    for message in client.list_job_messages(request=request):
        text = message.message_text or ""
        messages.append({"time": message.time.isoformat() if message.time else "", "text": text[-2500:]})
        if len(messages) >= limit:
            break
    return messages

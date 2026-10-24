"""Google Cloud clients, all built from one credential: the dfa-agent service account (impersonated).

The SA only has read roles plus create-only access to the landing bucket and its own incidents table,
so even a misbehaving agent cannot delete or overwrite anything (df-agent.md §7).
"""

from functools import cached_property

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class GcpClients:
    def __init__(self, gcp_cfg: dict):
        self.cfg = gcp_cfg
        self.project = gcp_cfg["project"]
        self.region = gcp_cfg["region"]

    @cached_property
    def credentials(self):
        import google.auth
        from google.auth import impersonated_credentials

        source, _ = google.auth.default(scopes=SCOPES)
        target = self.cfg.get("impersonate_service_account")
        if not target:
            return source
        return impersonated_credentials.Credentials(
            source_credentials=source, target_principal=target, target_scopes=SCOPES, lifetime=3600
        )

    @cached_property
    def bigquery(self):
        from google.cloud import bigquery

        return bigquery.Client(project=self.project, credentials=self.credentials, location=self.region)

    @cached_property
    def storage(self):
        from google.cloud import storage

        return storage.Client(project=self.project, credentials=self.credentials)

    @cached_property
    def logging(self):
        from google.cloud import logging as cloud_logging

        return cloud_logging.Client(project=self.project, credentials=self.credentials)

    @cached_property
    def dataflow_jobs(self):
        from google.cloud import dataflow_v1beta3

        return dataflow_v1beta3.JobsV1Beta3Client(credentials=self.credentials)

    @cached_property
    def dataflow_messages(self):
        from google.cloud import dataflow_v1beta3

        return dataflow_v1beta3.MessagesV1Beta3Client(credentials=self.credentials)

    @cached_property
    def workflow_executions(self):
        from google.cloud.workflows import executions_v1

        return executions_v1.ExecutionsClient(credentials=self.credentials)

    @cached_property
    def regions(self):
        from google.cloud import compute_v1

        return compute_v1.RegionsClient(credentials=self.credentials)

    @cached_property
    def resource_manager(self):
        from google.cloud import resourcemanager_v3

        return resourcemanager_v3.ProjectsClient(credentials=self.credentials)

    @cached_property
    def iam_admin(self):
        from google.cloud import iam_admin_v1

        return iam_admin_v1.IAMClient(credentials=self.credentials)

    @cached_property
    def subscriber(self):
        from google.cloud import pubsub_v1

        return pubsub_v1.SubscriberClient(credentials=self.credentials)

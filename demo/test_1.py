from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.auth.transport.requests import AuthorizedSession

target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
target_principal = "airflow-identity@artur-bolt-development.iam.gserviceaccount.com"
target_credentials = service_account.Credentials.from_service_account_file("path/to/keyfile.json", scopes=target_scopes)

delegated_credentials = impersonated_credentials.Credentials(
    source_credentials=target_credentials,
    target_principal=target_principal,
    target_scopes=target_scopes,
    lifetime=600
)

pod = client.V1Pod(
    metadata=client.V1ObjectMeta(name="example-pod"),
    spec=client.V1PodSpec(
        service_account_name="example-service-account",
        containers=[
            client.V1Container(
                name="example-container",
                image="bitnami/kubectl",
                command=["/bin/bash", "-c", "echo 'Hello, World!'"]
            )
        ]
    )
)

session = AuthorizedSession(delegated_credentials)
response = session.get("https://container.googleapis.com/v1/projects/example-project-id/zones/us-central1-a/clusters/example-cluster")
print(response.json())

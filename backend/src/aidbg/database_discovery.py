from __future__ import annotations

import json
from typing import Any

from .database_connectors import ConnectorUnavailable, DatabaseSafetyError


def discover_cloud_resources(
    provider: str,
    engine: str,
    config: dict[str, Any],
    credentials: dict[str, str],
) -> list[dict[str, str]]:
    injected = config.get("test_resources")
    if isinstance(injected, list):
        return [_resource(item.get("id", ""), item.get("name", ""), item.get("region", "")) for item in injected][:500]
    if provider == "aws":
        return _aws(engine, config, credentials)
    if provider == "gcp":
        return _gcp(engine, config, credentials)
    if provider == "azure":
        return _azure(engine, config, credentials)
    if provider == "oci":
        return _oci(engine, config, credentials)
    raise DatabaseSafetyError("automatic discovery requires AWS, GCP, Azure, or OCI")


def _resource(resource_id: Any, name: Any, region: Any = "") -> dict[str, str]:
    return {"id": str(resource_id), "name": str(name or resource_id), "region": str(region or "")}


def _aws(engine: str, config: dict[str, Any], credentials: dict[str, str]) -> list[dict[str, str]]:
    try:
        import boto3
    except ImportError as exc:
        raise ConnectorUnavailable("install boto3 for AWS resource discovery") from exc
    session = boto3.Session(
        aws_access_key_id=credentials.get("access_key_id") or None,
        aws_secret_access_key=credentials.get("secret_access_key") or None,
        aws_session_token=credentials.get("session_token") or None,
        region_name=config.get("region") or None,
    )
    resources: list[dict[str, str]] = []
    if engine in {"rds-postgresql", "rds-mysql", "rds-sqlserver", "rds-oracle"}:
        expected_engines = {
            "rds-postgresql": {"postgres"},
            "rds-mysql": {"mysql", "mariadb"},
            "rds-sqlserver": {"sqlserver-ee", "sqlserver-se", "sqlserver-ex", "sqlserver-web"},
            "rds-oracle": {"oracle-ee", "oracle-se2"},
        }[engine]
        pages = session.client("rds").get_paginator("describe_db_instances").paginate()
        for page in pages:
            for item in page.get("DBInstances", []):
                if item.get("Engine") not in expected_engines:
                    continue
                resources.append(_resource(item["DBInstanceIdentifier"], item["DBInstanceIdentifier"], item.get("AvailabilityZone")))
    elif engine in {"aurora-postgresql", "aurora-mysql", "documentdb"}:
        service = "docdb" if engine == "documentdb" else "rds"
        pages = session.client(service).get_paginator("describe_db_clusters").paginate()
        for page in pages:
            for item in page.get("DBClusters", []):
                resources.append(_resource(item["DBClusterIdentifier"], item["DBClusterIdentifier"], config.get("region")))
    elif engine == "dynamodb":
        pages = session.client("dynamodb").get_paginator("list_tables").paginate()
        for page in pages:
            resources.extend(_resource(name, name, config.get("region")) for name in page.get("TableNames", []))
    elif engine == "elasticache-redis":
        pages = session.client("elasticache").get_paginator("describe_cache_clusters").paginate()
        for page in pages:
            for item in page.get("CacheClusters", []):
                resources.append(_resource(item["CacheClusterId"], item["CacheClusterId"], item.get("PreferredAvailabilityZone")))
    elif engine == "redshift":
        pages = session.client("redshift").get_paginator("describe_clusters").paginate()
        for page in pages:
            for item in page.get("Clusters", []):
                resources.append(_resource(item["ClusterIdentifier"], item["ClusterIdentifier"], item.get("AvailabilityZone")))
    else:
        raise DatabaseSafetyError("the selected AWS engine does not support cloud discovery")
    return resources[:500]


def _gcp(engine: str, config: dict[str, Any], credentials: dict[str, str]) -> list[dict[str, str]]:
    try:
        from google.cloud import asset_v1
        from google.oauth2 import service_account
    except ImportError as exc:
        raise ConnectorUnavailable("install google-cloud-asset for GCP resource discovery") from exc
    project = str(config.get("project_id") or "")
    if not project:
        raise DatabaseSafetyError("GCP discovery requires project_id")
    credential = None
    if credentials.get("service_account_json"):
        credential = service_account.Credentials.from_service_account_info(
            json.loads(credentials["service_account_json"])
        )
    asset_types = {
        "cloudsql-postgresql": "sqladmin.googleapis.com/Instance",
        "cloudsql-mysql": "sqladmin.googleapis.com/Instance",
        "cloudsql-sqlserver": "sqladmin.googleapis.com/Instance",
        "alloydb": "alloydb.googleapis.com/Instance",
        "spanner": "spanner.googleapis.com/Instance",
        "firestore": "firestore.googleapis.com/Database",
        "memorystore-redis": "redis.googleapis.com/Instance",
    }
    asset_type = asset_types.get(engine)
    if not asset_type:
        raise DatabaseSafetyError("the selected GCP engine does not support cloud discovery")
    client = asset_v1.AssetServiceClient(credentials=credential)
    results = client.search_all_resources(
        request={"scope": f"projects/{project}", "asset_types": [asset_type], "page_size": 500}
    )
    resources = []
    for item in results:
        short_name = item.name.rsplit("/", 1)[-1]
        resource_id = f"{project}:{short_name}" if engine.startswith("cloudsql-") else short_name
        resources.append(_resource(resource_id, item.display_name or short_name, item.location))
    return resources[:500]


def _azure(engine: str, config: dict[str, Any], credentials: dict[str, str]) -> list[dict[str, str]]:
    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
        from azure.mgmt.resource import ResourceManagementClient
    except ImportError as exc:
        raise ConnectorUnavailable("install azure-mgmt-resource for Azure discovery") from exc
    subscription = str(config.get("subscription_id") or "")
    if not subscription:
        raise DatabaseSafetyError("Azure discovery requires subscription_id")
    credential = (
        ClientSecretCredential(credentials.get("tenant_id", ""), credentials.get("client_id", ""), credentials["client_secret"])
        if credentials.get("client_secret") else DefaultAzureCredential()
    )
    resource_types = {
        "azure-sql": "Microsoft.Sql/servers/databases",
        "azure-postgresql": "Microsoft.DBforPostgreSQL/flexibleServers",
        "azure-mysql": "Microsoft.DBforMySQL/flexibleServers",
        "cosmosdb": "Microsoft.DocumentDB/databaseAccounts",
        "azure-cache-redis": "Microsoft.Cache/Redis",
    }
    expected = resource_types.get(engine)
    if not expected:
        raise DatabaseSafetyError("the selected Azure engine does not support cloud discovery")
    client = ResourceManagementClient(credential, subscription)
    return [_resource(item.id, item.name, item.location) for item in client.resources.list() if str(item.type).lower() == expected.lower()][:500]


def _oci(engine: str, config: dict[str, Any], credentials: dict[str, str]) -> list[dict[str, str]]:
    try:
        import oci
    except ImportError as exc:
        raise ConnectorUnavailable("install oci for Oracle Cloud resource discovery") from exc
    compartment = str(config.get("compartment_id") or "")
    if not compartment:
        raise DatabaseSafetyError("OCI discovery requires compartment_id")
    oci_config = json.loads(credentials["config_json"]) if credentials.get("config_json") else oci.config.from_file(
        file_location=config.get("oci_config_file", oci.config.DEFAULT_LOCATION),
        profile_name=config.get("oci_profile", "DEFAULT"),
    )
    types = {"oci-autonomous": "AutonomousDatabase", "oci-database": "Database", "oci-nosql": "NosqlTable"}
    resource_type = types.get(engine)
    if not resource_type:
        raise DatabaseSafetyError("the selected OCI engine does not support cloud discovery")
    client = oci.resource_search.ResourceSearchClient(oci_config)
    query = f"query {resource_type} resources where compartmentId = '{compartment}'"
    response = client.search_resources(oci.resource_search.models.StructuredSearchDetails(query=query, type="Structured"), limit=500)
    return [_resource(item.identifier, item.display_name, item.availability_domain) for item in response.data.items][:500]

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request

import pymongo
import pymysql
from aidbg.database_connectors import connector_for


def retry(label, action, timeout=120):
    print(f"[wait] {label}", flush=True)
    deadline = time.monotonic() + timeout
    error = None
    while time.monotonic() < deadline:
        try:
            result = action()
            print(f"[ready] {label}", flush=True)
            return result
        except Exception as exc:
            error = exc
            time.sleep(2)
    raise RuntimeError(f"{label} was not ready: {error}")


def mysql(host, user="root", password="root-test", database=None):
    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        connect_timeout=5,
        autocommit=True,
    )


def initialize_mysql_primary():
    with mysql("mysql-primary") as connection, connection.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS orders")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS orders.events (id INT PRIMARY KEY, message VARCHAR(100))"
        )
        cursor.execute("CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl-test'")
        cursor.execute("GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'")
        cursor.execute("CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY 'monitor-test'")
        cursor.execute("GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO 'monitor'@'%'")
        cursor.execute("CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium-test'")
        cursor.execute(
            "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium'@'%'"
        )
        cursor.execute("CREATE USER IF NOT EXISTS 'cdc_writer'@'%' IDENTIFIED BY 'writer-test'")
        cursor.execute("GRANT INSERT ON orders.events TO 'cdc_writer'@'%'")


def initialize_mysql():
    retry("MySQL primary", initialize_mysql_primary)

    def replica_setup():
        with mysql("mysql-replica") as connection, connection.cursor() as cursor:
            cursor.execute("STOP REPLICA")
            cursor.execute("RESET REPLICA ALL")
            cursor.execute("RESET BINARY LOGS AND GTIDS")
            cursor.execute(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql-primary', SOURCE_USER='repl', SOURCE_PASSWORD='repl-test', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1"
            )
            cursor.execute("START REPLICA")
            cursor.execute("SET GLOBAL read_only=ON")
            cursor.execute("SET GLOBAL super_read_only=ON")

    retry("MySQL replica", replica_setup)

    def replication_ready():
        with (
            mysql("mysql-replica") as connection,
            connection.cursor(pymysql.cursors.DictCursor) as cursor,
        ):
            cursor.execute("SHOW REPLICA STATUS")
            status = cursor.fetchone() or {}
            if (
                status.get("Replica_IO_Running") != "Yes"
                or status.get("Replica_SQL_Running") != "Yes"
            ):
                raise RuntimeError(status)

    retry("MySQL replication", replication_ready)


def initialize_mongo():
    client = pymongo.MongoClient(
        "mongodb://mongo-1:27017", directConnection=True, serverSelectionTimeoutMS=3000
    )
    try:
        client.admin.command(
            "replSetInitiate",
            {
                "_id": "rs0",
                "members": [
                    {"_id": 0, "host": "mongo-1:27017", "priority": 2},
                    {"_id": 1, "host": "mongo-2:27017"},
                    {"_id": 2, "host": "mongo-3:27017"},
                ],
            },
        )
    except pymongo.errors.OperationFailure as exc:
        if exc.code != 23:
            raise
    finally:
        client.close()

    def ready():
        candidate = pymongo.MongoClient(
            "mongodb://mongo-2:27017", directConnection=True, serverSelectionTimeoutMS=3000
        )
        try:
            if not candidate.admin.command("hello").get("secondary"):
                raise RuntimeError("mongo-2 is not secondary")
        finally:
            candidate.close()

    retry("MongoDB replica set", ready)


def register_debezium():
    config = {
        "name": "orders-mysql",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "mysql-primary",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "debezium-test",
            "database.server.id": "184055",
            "topic.prefix": "dbserver1",
            "database.include.list": "orders",
            "table.include.list": "orders.events",
            "schema.history.internal.kafka.bootstrap.servers": "redpanda:9092",
            "schema.history.internal.kafka.topic": "schema-changes.orders",
            "include.schema.changes": "false",
            "snapshot.mode": "initial",
        },
    }

    def create():
        request = urllib.request.Request(
            "http://debezium:8083/connectors",
            data=json.dumps(config).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=10).read()
        except urllib.error.HTTPError as exc:
            if exc.code != 409:
                raise

    retry("Debezium REST API", create, timeout=180)

    def running():
        status = json.load(
            urllib.request.urlopen(
                "http://debezium:8083/connectors/orders-mysql/status", timeout=10
            )
        )
        if status["connector"]["state"] != "RUNNING" or any(
            task["state"] != "RUNNING" for task in status["tasks"]
        ):
            raise RuntimeError(status)

    retry("Debezium connector", running)


def replica(name, engine, host, port, credentials, database=""):
    return connector_for(
        name,
        "self_hosted",
        engine,
        "replica",
        {"host": host, "port": port, "database": database},
        credentials,
    )


def validate_replicas():
    def mongo_roles():
        roles = {}
        for host in ("mongo-1", "mongo-2", "mongo-3"):
            client = pymongo.MongoClient(
                f"mongodb://{host}:27017", directConnection=True, serverSelectionTimeoutMS=3000
            )
            try:
                hello = client.admin.command("hello")
                if hello.get("isWritablePrimary"):
                    roles["primary"] = host
                if hello.get("secondary"):
                    roles.setdefault("secondary", host)
            finally:
                client.close()
        if set(roles) != {"primary", "secondary"}:
            raise RuntimeError(roles)
        return roles

    roles = retry("MongoDB elected roles", mongo_roles)
    cases = [
        (
            "postgresql",
            replica(
                "postgres-primary",
                "postgresql",
                "postgres-primary",
                5432,
                {"username": "monitor", "password": "monitor-test"},
                "postgres",
            ),
            replica(
                "postgres-replica",
                "postgresql",
                "postgres-replica",
                5432,
                {"username": "monitor", "password": "monitor-test"},
                "postgres",
            ),
        ),
        (
            "mysql",
            replica(
                "mysql-primary",
                "mysql",
                "mysql-primary",
                3306,
                {"username": "monitor", "password": "monitor-test"},
                "orders",
            ),
            replica(
                "mysql-replica",
                "mysql",
                "mysql-replica",
                3306,
                {"username": "monitor", "password": "monitor-test"},
                "orders",
            ),
        ),
        (
            "mongodb",
            replica("mongo-primary", "mongodb", roles["primary"], 27017, {}),
            replica("mongo-secondary", "mongodb", roles["secondary"], 27017, {}),
        ),
        (
            "redis",
            replica("redis-primary", "redis", "redis-primary", 6379, {}),
            replica("redis-replica", "redis", "redis-replica", 6379, {}),
        ),
    ]
    output = {}
    for engine, primary, secondary in cases:
        print(f"[test] {engine} primary rejection and replica collection", flush=True)
        try:
            primary.verify()
        except Exception as exc:
            rejected = str(exc)
        else:
            raise AssertionError(f"{engine} primary was accepted")
        verification = retry(f"{engine} replica connector", secondary.verify)
        collection = secondary.collect()
        output[engine] = {
            "primary_rejected": rejected,
            "replica": verification,
            "signals": [signal.signal for signal in collection.signals],
        }
    return output


def validate_cdc():
    print("[test] Debezium CDC event", flush=True)
    connector = connector_for(
        "debezium-live",
        "self_hosted",
        "mysql",
        "cdc",
        {
            "bootstrap_servers": "redpanda:9092",
            "topic": "dbserver1.orders.events",
            "timeout_s": 10,
            "max_cycle_s": 10,
            "max_rows": 100,
        },
        {},
    )

    def insert_change(message):
        with mysql("mysql-primary", "cdc_writer", "writer-test", "orders") as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO events(id, message) VALUES (%s, %s)",
                    (time.time_ns() % 2_000_000_000, message),
                )

    # Debezium creates a table topic on the first captured row when the initial
    # snapshot is empty. Seed it before asking the connector to verify metadata.
    insert_change("create CDC topic")
    verification = retry("CDC topic", connector.verify)

    def insert():
        time.sleep(3)
        insert_change("repeatable live CDC test")

    thread = threading.Thread(target=insert, daemon=True)
    thread.start()
    collection = connector.collect()
    thread.join(10)
    operations = {signal.evidence.get("operation") for signal in collection.signals}
    assert collection.rows >= 1 and "c" in operations, collection.signals
    return {"verification": verification, "rows": collection.rows, "operations": sorted(operations)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("replicas", "cdc"))
    mode = parser.parse_args().mode
    if mode == "replicas":
        print("[setup] MySQL replication", flush=True)
        initialize_mysql()
        print("[setup] MongoDB replica set", flush=True)
        retry("MongoDB node", initialize_mongo)
        print(json.dumps({"replicas": validate_replicas()}, indent=2))
        return
    print("[setup] MySQL CDC source", flush=True)
    retry("MySQL primary", initialize_mysql_primary)
    print("[setup] Debezium connector", flush=True)
    register_debezium()
    print(json.dumps({"cdc": validate_cdc()}, indent=2))


if __name__ == "__main__":
    main()

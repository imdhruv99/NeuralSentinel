import sys
import time
import yaml
import argparse

from pathlib import Path

from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    ConfigResource,
    ConfigEntry,
    AlterConfigOpType,
)

from services.producer.config import ProducerConfig


def load_topic_definitions(path: Path) -> list[dict]:
    """
    Parse topics.yaml and return a list of topic definitions dict.

    I validate the bare minimum here. Name, Partitions, Replication Factor
    must be present. Everything else in config{} is passed straight to kafka
    as string key/value pairs (Kafka expects strings for topic-level configs)
    """

    raw = yaml.safe_load(path.read_text())
    topics = raw.get("topics", [])

    for t in topics:
        required_fields = ["name", "partitions", "replication_factor"]
        missing = set(required_fields) - set(t.keys())
        if missing:
            raise ValueError(
                f"Topic definition missing required fields: {missing} in {t}")
    return topics


def build_admin_client(config: ProducerConfig) -> AdminClient:
    """
    Create an AdminClient pointed at our broker list.

    librdkafka takes a flat config dict. bootstrap.servers accepts the raw
    comma-separated string. Admin operations carry their own per-call timeouts
    (request_timeout / operation_timeout), so there's nothing to tune here for
    the local Docker startup window beyond pointing at the brokers.
    """

    return AdminClient(
        {
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "client.id": "neural-sentinel-topic-admin",
        }
    )


def sync_topics(admin: AdminClient, definitions: list[dict]) -> None:
    """
    Idempotent sync: create topics that don't exist, update config on those that do.

    Kafka's AdminClient separates "create topic" from "alter topic config" —
    they're two different API calls. I have handled each case explicitly:

    1. Fetch existing topics from the cluster.
    2. For topics not yet in the cluster -> create them (partitions + replication
       are set at creation time and can't be changed after).
    3. For topics that already exist -> alter their config map. I deliberately
       do NOT attempt to change partition count or replication factor on existing
       topics because Kafka requires a full delete+recreate for that, which is
       destructive. If you need to change those, run `make topics-delete` first.

    confluent-kafka's admin calls are async: each returns a dict {name: Future}.
    The request only really succeeds/fails when you call future.result(), which
    re-raises as a KafkaException carrying a KafkaError code.
    """

    # list_topics() returns cluster metadata (includes internal topics like
    # __consumer_offsets); we only diff against our own declared names.
    existing_topics = set(admin.list_topics(timeout=10).topics.keys())
    print(f"Cluster currently has {len(existing_topics)} topic(s).")

    to_create = []
    to_update = []

    for defs in definitions:
        name = defs["name"]
        topic_config = {k: str(v) for k, v in defs.get("config", {}).items()}

        if name not in existing_topics:
            to_create.append(
                NewTopic(
                    name,
                    num_partitions=defs["partitions"],
                    replication_factor=defs["replication_factor"],
                    config=topic_config,
                )
            )
        else:
            to_update.append((name, topic_config))

    # Create new topics
    if to_create:
        print(f"\n Creating {len(to_create)} new topic(s):")
        for t in to_create:
            print(
                f"  + {t.topic}  (partitions={t.num_partitions}, rf={t.replication_factor})")
        futures = admin.create_topics(to_create)
        for name, fut in futures.items():
            try:
                fut.result()  # blocks until the controller acks this topic
                print(f"  created {name}")
            except KafkaException as exc:
                # Race condition: another process created it between our list and
                # create. Safe to ignore — the topic exists, which is the goal.
                if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"  {name} already existed (race) - continuing.")
                else:
                    print(f" Create failed for {name}: {exc}", file=sys.stderr)
                    raise
    else:
        print("\nNo new topics to create.")

    # Update configs for existing topics
    if to_update:
        print(f"\n Updating config on {len(to_update)} existing topic(s):")
        # incremental_alter_configs only touches the keys we name (SET each one)
        # and leaves every other broker-managed config untouched — unlike the
        # deprecated alter_configs, which replaced the whole config map.
        resources = [
            ConfigResource(
                ConfigResource.Type.TOPIC,
                name,
                incremental_configs=[
                    ConfigEntry(
                        k, v, incremental_operation=AlterConfigOpType.SET)
                    for k, v in cfg_map.items()
                ],
            )
            for name, cfg_map in to_update
        ]
        futures = admin.incremental_alter_configs(resources)
        for res, fut in futures.items():
            try:
                fut.result()
                print(f"  ~ {res.name} config updated.")
            except KafkaException as exc:
                print(f" Config update failed for {res.name}: {exc}",
                      file=sys.stderr)
                raise
    else:
        print("No existing topics to update.")


def delete_topics(admin: AdminClient, definitions: list[dict]) -> None:
    """
    Delete all topics declared in topics.yaml.

    This is destructive - all data in the topics is lost. It's here for the
    dev workflow where you want a clean slate (e.g. after changing partition
    count, which requires delete + recreate).

    operation_timeout asks the broker to wait until the topics are actually
    gone (not just marked) before completing the future. A short sleep after
    still helps cluster metadata propagate before an immediate recreate.
    """
    names = [d["name"] for d in definitions]
    print(f"Deleting topics: {names}")

    futures = admin.delete_topics(names, operation_timeout=30)
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"  deleted {name}")
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"  {name} didn't exist - skipping.")
            else:
                print(f" Delete failed for {name}: {exc}", file=sys.stderr)
                raise

    print("Waiting 2s for broker metadata to settle...")
    time.sleep(2)
    print("Done.")


def list_topics(admin: AdminClient) -> None:
    """Print all topics currently in the cluster."""
    topics = sorted(admin.list_topics(timeout=10).topics.keys())
    print(f"Cluster topics ({len(topics)}):")
    for t in topics:
        print(f"  {t}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage NeuralSentinel Kafka topics declaratively.",
        epilog="Example: python topic_admin.py sync",
    )
    parser.add_argument(
        "action",
        choices=["sync", "delete", "list"],
        help=(
            "sync: create/update topics from topics.yaml | "
            "delete: remove all declared topics | "
            "list: show current cluster topics"
        ),
    )
    args = parser.parse_args()

    cfg = ProducerConfig()
    topics_file = Path(__file__).parent / "topics.yaml"
    definitions = load_topic_definitions(topics_file)

    admin = build_admin_client(cfg)
    # confluent-kafka's AdminClient has no close(); it's released on GC.
    if args.action == "sync":
        sync_topics(admin, definitions)
    elif args.action == "delete":
        delete_topics(admin, definitions)
    elif args.action == "list":
        list_topics(admin)


if __name__ == "__main__":
    main()

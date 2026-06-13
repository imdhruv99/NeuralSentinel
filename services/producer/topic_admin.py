import sys
import time
import yaml
import argparse

from pathlib import Path

from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, UnknownTopicOrPartitionError

from config import ProducerConfig


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
        missing = required_fields - set(t.keys())
        if missing:
            raise ValueError(f"Topic definition missing required fields: {missing} in {t}")
    return topics


def build_admin_client(config: ProducerConfig) -> KafkaAdminClient:
    """
    Create an AdminClient pointed at our broker list.

    retry_backoff_ms and request_timeout_ms are tuned for local Docker:
    brokers are on the same machine so latency is negligible, but during
    startup there's a window where brokers are up but not yet ready to
    accept admin requests. A generous timeout avoids false failures.
    """

    return KafkaAdminClient(
        bootstrap_servers=config.kafka_bootstrap_servers,
        client_id="neural-sentinel-topic-admin",
        request_timeout_ms=30_000,
        retry_backoff_ms=500,
    )


def sync_topics(admin: KafkaAdminClient, definitions: list[dict]) -> None:
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
    """

    existing_topics = set(admin.list_topics())
    print(f"Cluster currently has {len(existing_topics)} topic(s).")

    to_create = []
    to_update = []

    for defs in definitions:
        name = defs["name"]
        topic_config = {k: str(v)for k, v in defs.get("config", {}).items()}

        if name not in existing_topics:
            to_create.append(
                NewTopic(
                    name=name,
                    num_partitions=defs["partitions"],
                    replication_factor=defs["replication_factor"],
                    topic_configs=topic_config,
                )
            )
        else:
            to_update.append((name, topic_config))

    # Create new topics
    if to_create:
        print(f"\n Creating {len(to_create)} new topic(s):")
        for t in to_create:
            print(f"  + {t.name}  (partitions={t.num_partitions}, rf={t.replication_factor})")
        try:
            admin.create_topics(new_topics=to_create, validate_only=False)
            print(" Topics are Created successfully.")
        except TopicAlreadyExistsError:
            # Race condition: another process created it between our list and create.
            # Safe to ignore - the topic exists, which is what we wanted.
            print(" Some topics already existed (race condition) — continuing.")
        except KafkaError as exc:
            print(f" Create failed: {exc}", file=sys.stderr)
            raise
    else:
        print("\nNo new topics to create.")

    # Update configs for existing topics
    if to_update:
        print(f"\n Updating config on {len(to_update)} existing topic(s):")
        for name, cfg_map in to_update:
            print(f"  ~ {name}")
            resources = [
                ConfigResource(
                    resource_type=ConfigResourceType.TOPIC,
                    name=name,
                    configs=cfg_map,
                )
            ]
            try:
                admin.alter_configs(resources)
                print(f" Config updated.")
            except KafkaError as exc:
                print(f" Config update failed: {exc}", file=sys.stderr)
                raise
    else:
        print("No existing topics to update.")


def delete_topics(admin: KafkaAdminClient, definitions: list[dict]) -> None:
    """
    Delete all topics declared in topics.yaml.

    This is destructive - all data in the topics is lost. It's here for the
    dev workflow where you want a clean slate (e.g. after changing partition
    count, which requires delete + recreate).

    Sleep briefly after deletion because Kafka's delete is asynchronous:
    the broker marks the topic for deletion and the actual log cleanup happens
    in the background. If you immediately call create after delete you may hit
    a "topic already exists" error from the lingering metadata. 2 seconds is
    usually enough in a local Docker setup.
    """
    names = [d["name"] for d in definitions]
    print(f"Deleting topics: {names}")

    try:
        admin.delete_topics(names)
        print("Delete request sent. Waiting 2s for broker to process...")
        time.sleep(2)
        print("Done.")
    except UnknownTopicOrPartitionError:
        print("Some topics didn't exist — nothing to delete.")
    except KafkaError as exc:
        print(f"Delete failed: {exc}", file=sys.stderr)
        raise


def list_topics(admin: KafkaAdminClient) -> None:
    """Print all topics currently in the cluster."""
    topics = sorted(admin.list_topics())
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
    try:
        if args.action == "sync":
            sync_topics(admin, definitions)
        elif args.action == "delete":
            delete_topics(admin, definitions)
        elif args.action == "list":
            list_topics(admin)
    finally:
        admin.close()


if __name__ == "__main__":
    main()

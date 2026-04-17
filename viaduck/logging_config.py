import logging
import os
import sys


def setup() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    instance_id = os.environ.get("VIADUCK_INSTANCE_ID", "")
    prefix = f"[{instance_id}]" if instance_id else ""
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s %(levelname)-5s {prefix}[%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )

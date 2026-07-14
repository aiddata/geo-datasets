"""
Create or update the Kubernetes work pool that geo-datasets flows run on.

The pool's base job template lives in base-job-template.json, which already
carries the SciClone volume mount and the securityContext that flow pods need.
This script applies that template, overriding a few values that are handy to
set per-environment (namespace, image, concurrency).

Usage:
    python create-work-pool.py --namespace geo-datasets
"""

import asyncio
import json
from pathlib import Path

import click
from prefect import get_client
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
from prefect.exceptions import ObjectAlreadyExists

BASE_JOB_TEMPLATE = Path(__file__).parent / "base-job-template.json"


async def create_work_pool(wp_kwargs):
    async with get_client() as client:
        try:
            wp = WorkPoolCreate(**wp_kwargs)
            await client.create_work_pool(work_pool=wp)
            print("Created work pool {}".format(wp_kwargs["name"]))
        except ObjectAlreadyExists:
            name = wp_kwargs.pop("name")
            # TODO: assert that existing work pool has matching type (kubernetes)
            del wp_kwargs["type"]
            await client.update_work_pool(
                work_pool_name=name, work_pool=WorkPoolUpdate(**wp_kwargs)
            )
            print(f"Work pool {name} updated")


@click.command()
@click.option("--pool-name", default="geodata", help="Name of work pool", type=str)
@click.option(
    "--concurrency-limit",
    default=None,
    help="Concurrency maximum for work pool",
    type=int,
)
@click.option(
    "--namespace",
    default="geo-datasets",
    help="Kubernetes namespace to run flow jobs in",
    type=str,
)
@click.option(
    "--image",
    default=None,
    help="Image to run jobs in. Defaults to the value in base-job-template.json.",
    type=str,
)
@click.option(
    "--persistent-volume-claim",
    default=None,
    help="PersistentVolumeClaim to mount at /sciclone/nova/REU/geo. Defaults to the value in base-job-template.json.",
    type=str,
)
def main(pool_name, concurrency_limit, namespace, image, persistent_volume_claim):
    with open(BASE_JOB_TEMPLATE) as src:
        base_job_template = json.load(src)

    properties = base_job_template["variables"]["properties"]
    properties["namespace"]["default"] = namespace

    if image:
        properties["image"]["default"] = image

    if persistent_volume_claim:
        pod_spec = base_job_template["job_configuration"]["job_manifest"]["spec"][
            "template"
        ]["spec"]
        pod_spec["volumes"][0]["persistentVolumeClaim"][
            "claimName"
        ] = persistent_volume_claim

    wp_kwargs = {
        "name": pool_name,
        "description": "Work pool for geodata k8s deployment",
        "type": "kubernetes",
        "base_job_template": base_job_template,
        "is_paused": False,
        "concurrency_limit": concurrency_limit,
    }

    asyncio.run(create_work_pool(wp_kwargs))


if __name__ == "__main__":
    main()

import json
import asyncio

import click

from prefect import get_client
from prefect.exceptions import ObjectAlreadyExists
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate

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
                work_pool_name=name,
                work_pool=WorkPoolUpdate(**wp_kwargs)
            )
            print(
                f"Work pool {name} updated"
            )

@click.command()
@click.option("--pool-name", default="geodata-pool", help="Name of work pool", type=str)
@click.option("--concurrency-limit", default=None, help="Concurrency maximum for work pool", type=int)
@click.option("--namespace", required=True, help="Name of k8s namespace", type=str)
@click.option("--image", default="jacobwhall/geodata-container", help="Name of image to run jobs in", type=str)
@click.option("--cpu-request", default=None, help="CPU request", type=int)
@click.option("--cpu-limit", default=None, help="CPU limit", type=int)
@click.option("--memory-request", default=None, help="Memory request", type=int)
@click.option("--memory-limit", default=None, help="Memory limit", type=int)
def main(pool_name, concurrency_limit, namespace, image, cpu_request, cpu_limit, memory_request, memory_limit):

    # should we have requests and limits?
    requests_and_limits: bool = False

    with open("base-job-template.json") as src:
        base_job_template = json.loads(src.read())

    # set default namespace
    base_job_template["variables"]["properties"]["namespace"]["default"] = namespace

    # set default image
    base_job_template["variables"]["properties"]["image"]["default"] = image

    # TODO: set cpu request

    # TODO: set cpu limit

    # TODO: set memory request

    # TODO: set memory limit

    # whether or not to use volume
    use_volume = True

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

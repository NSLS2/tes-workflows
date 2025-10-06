from prefect import flow, task, get_run_logger
from tiled_utils import get_tiled_client

from exporters import export_E_step, export_E_fly

tiled_client = get_tiled_client()["raw"]

processor_map = {
    'export_E_step': export_E_step,
    'export_E_fly': export_E_fly,
}

@task
def dispatcher(run_uid):
    logger = get_run_logger()
    run = tiled_client[run_uid]
    for processor in run.start["prefect_post_processors"]:
        logger.info(f"Start post-processor '{processor}'...")
        processor_map[processor](run)
        logger.info(f"Finish post-processor '{processor}'")

@flow(log_prints=True)
def post_processors(run_uid):
    logger = get_run_logger()
    logger.info("Start post_processors...")
    dispatcher(run_uid)
    logger.info("Finish post_processors.")

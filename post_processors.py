from prefect import flow, task, get_run_logger

from data_validation import get_run
from exporters import export_E_step, export_E_fly

BEAMLINE_ACRONYM = "tes"


processor_map = {
    'export_E_step': export_E_step,
    'export_E_fly': export_E_fly,
}

@task
def dispatcher(run_uid, api_key=None):
    logger = get_run_logger()
    run = get_run(run_uid, api_key=api_key)
    for processor in run.start["prefect_post_processors"]:
        logger.info(f"Start post-processor '{processor}'...")
        processor_map[processor](run)
        logger.info(f"Finish post-processor '{processor}'")


@flow(log_prints=True)
def post_processors(run_uid, api_key=None):
    logger = get_run_logger()
    logger.info("Start post_processors...")
    dispatcher(run_uid, api_key=api_key)
    logger.info("Finish post_processors.")

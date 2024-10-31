from prefect import flow, task, get_run_logger
from tiled.client import from_profile

from export_E_step import export_E_step

BEAMLINE_ACRONYM = "tes"

tiled_client = from_profile("nsls2")[BEAMLINE_ACRONYM]["raw"]

processor_map = {
    'export_E_step': export_E_step,
    'export_Esmart_step': export_E_step
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

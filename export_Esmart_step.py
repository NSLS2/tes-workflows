from prefect import flow, task, get_run_logger

@flow(log_prints=True)
def export_Esmart_step(run):
    logger = get_run_logger()
    scan_index = run.start["scan_index"]
    logger.info(f"scan_index: {scan_index}")

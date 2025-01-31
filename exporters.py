from prefect import flow, task, get_run_logger
import pandas as pd
import datetime
import os
import pprint
import numpy as np

from utils import get_proposal_dir, get_detector, get_rois


def export_E_step(run):
    logger = get_run_logger()
    scan_index = run.start["scan_index"]

    E = run.start["user_input"]["E_points"]

    I0 = run["primary"]["data"]["I0"].read()
    It = run["primary"]["data"]["It"].read()
    I_TEY = run["primary"]["data"]["fbratio"].read()

    # This will raise an exception if detector is not xs or xssmart.
    detector = get_detector(run)

    if detector == "xs":
        If_1_roi1 = run["primary"]["data"]["xs_channel01_mcaroi01_total_rbv"].read()
        If_1_roi2 = run["primary"]["data"]["xs_channel01_mcaroi02_total_rbv"].read()
        If_1_roi3 = run["primary"]["data"]["xs_channel01_mcaroi03_total_rbv"].read()
        If_1_roi4 = run["primary"]["data"]["xs_channel01_mcaroi04_total_rbv"].read()

        df = pd.DataFrame(
            {
                "#Energy": E,
                "I0": I0,
                "I_TEY": I_TEY,
                "If_CH1_roi1": If_1_roi1,
                "If_CH1_roi2": If_1_roi2,
                "If_CH1_roi3": If_1_roi3,
                "If_CH1_roi4": If_1_roi4,
            }
        )
    else:
        If_1_roi1 = run["primary"]["data"][
            "xssmart_channel01_mcaroi01_total_rbv"
        ].read()
        If_2_roi1 = run["primary"]["data"][
            "xssmart_channel02_mcaroi01_total_rbv"
        ].read()
        If_3_roi1 = run["primary"]["data"][
            "xssmart_channel03_mcaroi01_total_rbv"
        ].read()
        If_4_roi1 = run["primary"]["data"][
            "xssmart_channel04_mcaroi01_total_rbv"
        ].read()

        df = pd.DataFrame(
            {
                "#Energy": E,
                "I0": I0,
                "It": It,
                "I_TEY": I_TEY,
                "If_CH1_roi1": If_1_roi1,
                "If_CH2_roi1": If_2_roi1,
                "If_CH3_roi1": If_3_roi1,
                "If_CH4_roi1": If_4_roi1,
            }
        )

    start = run.start
    dt = datetime.datetime.fromtimestamp(start["time"])

    user_input = start['user_input']
    del user_input['E_points']

    file_head = {
        "beamline_id": "TES/8-BM of NSLS-II",
        "operator": start["operator"],
        "plan_name": start["plan_name"],
        "scan_id": start["scan_id"],
        "scan_title": start["scan_title"],
        "time": f"{dt.date().isoformat()} {dt.time().isoformat()}",
        "uid": start["uid"],
        "user_input": user_input,
        "derived_input": start["derived_input"],
    }

    working_dir = (
        get_proposal_dir(run)
        / f"Data/{run.start['operator']}/{dt.date().isoformat()}/E_step"
    )
    filename = f"{start['scan_title']}-{start['scan_id']}-{start['operator']}-{dt.time().strftime('%H-%M-%S')}-{scan_index}.cvs"
    filepath = working_dir / filename

    os.makedirs(working_dir, exist_ok=True)

    with open(filepath, "wt") as output_file:
        output_file.write(pprint.pformat(file_head, width=100))
        output_file.write("\n")
        output_file.write("\n")
        output_file.write("\n")

    df.to_csv(filepath, header=True, index=False, mode="a")
    logger.info(f"Data exported to {filepath}")


def export_E_fly(run):
    logger = get_run_logger()
    start = run.start

    roi = get_rois(run)

    d = run["primary"]["data"]["fluor"].read()
    If = np.sum(d[:, :, :, roi[0] : roi[0] + roi[1]], axis=-1)

    primary_data = run["primary"]["data"]
    I_TEY = primary_data["fbratio"].read()[0]
    E = run["energy_bins"]["data"]["E_centers"].read()[0]
    I0 = primary_data["I0"].read()[0]
    It = run["primary"]["data"]["It"].read()[0]
    Dwell_time = primary_data["dwell_time"].read()[0]

    dt = datetime.datetime.fromtimestamp(start["time"])

    file_head = {
        "beamline_id": "TES/8-BM of NSLS-II",
        "operator": start["operator"],
        "plan_name": start["plan_name"],
        "scan_id": start["scan_id"],
        "scan_title": start["scan_title"],
        "time": f"{dt.date().isoformat()} {dt.time().isoformat()}",
        "uid": start["uid"],
        "user_input": start["user_input"],
        "derived_input": start["derived_input"],
    }

    # This will raise an exception if detector is not xs or xssmart.
    detector = get_detector(run)

    for ii in range(If.shape[0]):

        if detector == "xs":

            df = pd.DataFrame(
                {
                    "#Energy": E,
                    "Dwell_time": Dwell_time[ii + 1],
                    "I0": I0[ii + 1],
                    "I_TEY": I_TEY[ii + 1],
                    "If_CH1": If[ii, :, 0],
                }
                | (
                    {
                        "If_CH2": If[ii, :, 1],
                    }
                    if If.shape[2] != 1
                    else {}
                )
            )

        else:  # xssmart
            df = pd.DataFrame(
                {
                    "#Energy": E,
                    "Dwell_time": Dwell_time[ii + 1],
                    "I0": I0[ii + 1],
                    "I_TEY": I_TEY[ii + 1],
                    "If_CH1": If[ii, :, 0],
                    "If_CH2": If[ii, :, 1],
                    "If_CH3": If[ii, :, 2],
                    "If_CH4": If[ii, :, 3],
                    "It": It[ii + 1],
                }
            )

        working_dir = (
            get_proposal_dir(run)
            / f"Data/{start['operator']}/{dt.date().isoformat()}/E_fly/"
        )
        filename = f"{start['scan_title']}-{start['scan_id']}-{start['operator']}-{dt.time().strftime('%H-%M-%S')}-{ii}.dat"
        filepath = working_dir / filename

        os.makedirs(working_dir, exist_ok=True)

        with open(filepath, "wt") as output_file:
            output_file.write(pprint.pformat(file_head, width=100))
            output_file.write("\n")
            output_file.write("\n")
            output_file.write("\n")

        df.to_csv(filepath, header=True, index=False, mode="a")
        logger.info(f"Data exported to {filepath}")

    logger.info("Export complete.")

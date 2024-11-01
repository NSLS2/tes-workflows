from pathlib import Path


element_to_roi = {
    # channel-start, width, absorption-edge
    "xs": {
        "ru_l3": (256, 16, 2838),
    },
    "xssmart": {
        "cl_k": (256, 14, 2822),
        "s": (224, 12, 2472),
        "p_k": (196, 10, 2145.5),
        "pb_m3": (260, 14, 3066),
        "ru_l3": (256, 16, 2838),
        "ru_l2": (260, 14, 2967),
        "ru_l1": (289, 15, 3224),
        "ir_m5": (190, 14, 2070),
        "ti_k": (444, 12, 4966),
        "s_k": (224, 12, 2472),
    },
}


def get_proposal_dir(run):
    if (
        "Routine Setup and Testing For Beamline 8‐BM".lower()
        in run.start["proposal"]["title"].lower()
    ):
        proposal_dir = Path(
            f"/nsls2/data/tes/proposals/commissioning/{run.start['data_session']}"
        )
    else:
        proposal_dir = Path(
            f"/nsls2/data/tes/proposals/{run.start['cycle']}/{run.start['data_session']}"
        )  # noqa: E501

    return proposal_dir


def get_detector(run):
    if "xs" in run.start["detectors"]:
        return "xs"
    if "xssmart" in run.start["detectors"]:
        return "xssmart"
    else:
        raise ValueError(
            "Run must have either 'xs' or 'xssmart' detector.  "
            f"This run has detectors: {run.start['detectors']}"
        )


def get_rois(run):
    detector = get_detector(run)
    element = run.start["user_input"]["element"]
    edge = run.start["user_input"]["edge"]
    return element_to_roi[detector][f"{element}_{edge}".lower()]

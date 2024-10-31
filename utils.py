from pathlib import Path

def get_proposal_dir(run):
    if "Routine Setup and Testing For Beamline 8‐BM".lower() in run.start["proposal"]["title"].lower():
        proposal_dir = Path(f"/nsls2/data/tes/proposals/commissioning/{run.start['data_session']}")
    else:
        proposal_dir = Path(f"/nsls2/data/tes/proposals/{run.start['cycle']}/{run.start['data_session']}")  # noqa: E501
    
    return proposal_dir

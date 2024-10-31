from prefect import flow, task, get_run_logger

@flow(log_prints=True)
def export_E_step(run):
    logger = get_run_logger()

    scan_index = run.start["scan_index"]

    e_back = yield from _get_v_with_dflt(mono.e_back, 1977.04)
    energy_cal = yield from _get_v_with_dflt(mono.cal, 0.40118)

    def _linear_to_energy(linear):
        linear = np.asarray(linear)
        return e_back / np.sin(
            np.deg2rad(45)
            + 0.5 * np.arctan((28.2474 - linear) / 35.02333)
            + np.deg2rad(energy_cal) / 2
        )

    E = _linear_to_energy(run["primary"]["data"]["mono_linear"].read())

    # E = h.table()['mono_energy']
    I0 = run["primary"]["data"]["I0"].read()
    I_TEY = run["primary"]["data"]["fbratio"].read()
    # If_1_roi1 = h.table()['xs_channel1_rois_roi01_value_sum']
    # If_1_roi2 = h.table()['xs_channel1_rois_roi02_value_sum']
    # If_1_roi3 = h.table()['xs_channel1_rois_roi03_value_sum']
    # If_1_roi4 = h.table()['xs_channel1_rois_roi04_value_sum']

    If_1_roi1 = run["primary"]["data"][xs.channel01.mcaroi01.total_rbv.name].read()
    If_1_roi2 = run["primary"]["data"][xs.channel01.mcaroi02.total_rbv.name].read()
    If_1_roi3 = run["primary"]["data"][xs.channel01.mcaroi03.total_rbv.name].read()
    If_1_roi4 = run["primary"]["data"][xs.channel01.mcaroi04.total_rbv.name].read()

    # If_2_roi1 = h.table()['xs_channel2_rois_roi01_value_sum']
    # If_2_roi2 = h.table()['xs_channel2_rois_roi02_value_sum']
    # If_2_roi3 = h.table()['xs_channel2_rois_roi03_value_sum']
    # If_2_roi4 = h.table()['xs_channel2_rois_roi04_value_sum']

    # df = pd.DataFrame({'#Energy': E, 'I0': I0, 'I_TEY':I_TEY,
    #                   'If_CH1_roi1': If_1_roi1, 'If_CH1_roi2': If_1_roi2, 'If_CH1_roi3':If_1_roi3, 'If_CH1_roi4': If_1_roi4,
    #                   'If_CH2_roi1': If_2_roi1, 'If_CH2_roi2': If_2_roi2, 'If_CH2_roi3':If_2_roi3, 'If_CH2_roi4': If_2_roi4})
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
    # df['#Energy'] = df1['#Energy'].str.rjust(13, " ")

    start = run.start
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

    filepath = os.path.expanduser(
        f"~/Users/Data/{start['operator']}/{dt.date().isoformat()}/E_step/"
        f"{start['scan_title']}-{start['scan_id']}-{start['operator']}-{dt.time().strftime('%H-%M-%S')}-{scan_index}.cvs"
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "wt") as output_file:
        output_file.write(pprint.pformat(file_head, width=100))
        output_file.write("\n")
        output_file.write("\n")
        output_file.write("\n")

    df.to_csv(filepath, header=True, index=False, mode="a")
    print(f"Data exported to {filepath}")
    # Save to Lustre
    filepath = os.path.expanduser(
        f"/nsls2/data/tes/legacy/usersdata/Data/{start['operator']}/{dt.date().isoformat()}/E_step/"
        f"{start['scan_title']}-{start['scan_id']}-{start['operator']}-{dt.time().strftime('%H-%M-%S')}-{scan_index}.cvs"
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "wt") as output_file:
        output_file.write(pprint.pformat(file_head, width=100))
        output_file.write("\n")
        output_file.write("\n")
        output_file.write("\n")

    df.to_csv(filepath, header=True, index=False, mode="a")
    print(f"Data exported to {filepath}")
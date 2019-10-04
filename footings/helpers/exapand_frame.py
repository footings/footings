def expand_frame_by_dates(df, col_nm, start_dt, end_dt, freq, **kwargs):
    """

    """
    assert df.shape[0] == 1
    start = df[start_dt][0]
    end = df[end_dt][0]
    df[col_nm] = [pd.date_range(start=start, end=end, freq=freq)]

    lens = [len(x) for x in df[col_nm]]
    d = {c: np.repeat(df[c].values, lens) for c in df.drop(col_nm, axis=1).columns}
    z = {col_nm: np.concatenate(df[col_nm].values)}

    df = pd.DataFrame({**d, **z})

    if "calendar_year" in kwargs:
        df[kwargs["calendar_year"]] = df[col_nm].dt.year

    if "calendar_quarter" in kwargs:
        df[kwargs["calendar_quarter"]] = df[col_nm].dt.quarter

    if "calendar_month" in kwargs:
        df[kwargs["calendar_month"]] = df[col_nm].dt.month

    if "calendar_day" in kwargs:
        df[kwargs["calendar_day"]] = df[col_nm].dt.day

    if "duration_year" in kwargs:
        df[kwargs["duration_year"]] = (
            np.floor((df[col_nm] - start) / np.timedelta(1, "Y")) + 1
        )

    if "duration_quarter" in kwargs:
        df[kwargs["duration_year"]] = (
            np.floor((df[col_nm] - start) / np.timedelta(1, "Q")) + 1
        )

    if "duration_month" in kwargs:
        df[kwargs["duration_year"]] = (
            np.floor((df[col_nm] - start) / np.timedelta(1, "M")) + 1
        )

    if "duration_day" in kwargs:
        df[kwargs["duration_day"]] = (
            df[col_nm].dt.to_period("Y") - start.to_period("Y")
        ).apply(attrgetter("n"))

    return df

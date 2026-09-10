"""Shared method display labels for experiment plots."""

METHOD_DISPLAY_LABELS = {
    "fast": "SW",
    "baseline1": "SW",
    "delayed": "DWW",
    "baseline2": "DWW",
    "baseline": "ALW",
    "baseline3": "ALW",
    "proposed": "CBW",
}

METHOD_DISPLAY_ORDER = ("SW", "DWW", "ALW", "CBW")

_METHOD_ORDER = {
    "fast": 0,
    "baseline1": 0,
    "sw": 0,
    "delayed": 1,
    "baseline2": 1,
    "dww": 1,
    "baseline": 2,
    "baseline3": 2,
    "alw": 2,
    "proposed": 3,
    "cbw": 3,
}


def method_label(value: str) -> str:
    key = str(value).strip()
    return METHOD_DISPLAY_LABELS.get(key.lower(), key)


def method_sort_key(value: str) -> int:
    return _METHOD_ORDER.get(str(value).strip().lower(), len(_METHOD_ORDER))


def ordered_methods(values):
    return sorted(values, key=method_sort_key)


def sort_by_method_order(df, column: str):
    if df.empty or column not in df.columns:
        return df
    order_col = "__method_order"
    return (
        df.assign(**{order_col: df[column].map(method_sort_key)})
        .sort_values([order_col, column])
        .drop(columns=[order_col])
    )

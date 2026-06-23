def flatten_feature_map(feature_map: dict[str, dict[str, float]]) -> dict[str, float]:
    """
    Flatten a nested {metric -> {stat -> value}} map into a single
    {metric__stat -> value} dict. The double-underscore separator is the
    contract used by the training pipeline to identify feature columns
    (any column with '__' in its name is a feature).
    """
    row: dict[str, float] = {}
    for metric_name, stats_map in feature_map.items():
        for stat_name, value in stats_map.items():
            row[f"{metric_name}__{stat_name}"] = value
    return row

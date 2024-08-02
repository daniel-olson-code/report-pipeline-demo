def transform_report(report_data: list[dict]) -> list[dict]:
    """
    Transforms a report by calculating and adding the Average Order Value (AOV) for each row.

    This function iterates through the report data and calculates the AOV
    by dividing sales by the number of orders. If there are no orders, AOV is set to 0.

    Args:
        report_data (list[dict]): A list of dictionaries, where each dictionary
            represents a row in the report and contains 'sales' and 'orders' keys.

    Returns:
        list[dict]: The transformed report data with an additional 'aov' key in each row.

    Example:
        original_data = [{'sales': 1000, 'orders': 10}, {'sales': 500, 'orders': 5}]
        transformed_data = transform_report(original_data)
        # Result: [{'sales': 1000, 'orders': 10, 'aov': 100}, {'sales': 500, 'orders': 5, 'aov': 100}]
    """
    for row in report_data:
        row['aov'] = row['sales'] / row['orders'] if row['orders'] > 0 else 0

    return report_data


# def transform_report(report_data: list[dict]) -> list[dict]:
#
#     for row in report_data:
#         row['aov'] = row['sales'] / row['orders'] if row['orders'] > 0 else 0
#
#     return report_data
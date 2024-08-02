import datetime

import requests


def request_report(url: str, region: str, report: str, end_date: datetime.datetime | str | None) -> str:
    """
    Requests a report from a specified URL for a given region and report type.

    This function sends a POST request to create a report for a 50-day period
    ending on the specified end date (or current date if not provided).

    Args:
        url (str): The base URL for the report API.
        region (str): The region for which the report is requested.
        report (str): The type of report to be generated.
        end_date (datetime.datetime | str | None): The end date for the report period.
            If None, the current date is used. If a string, it should be in 'YYYY-MM-DD' format.

    Returns:
        str: The report ID returned by the API.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request returns an unsuccessful status code.

    Example:
        report_id = request_report('https://api.example.com', 'NA', 'sales', '2023-12-31')
    """
    if end_date is None:
        end_date = datetime.datetime.now()
    elif isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    r = requests.post(f'{url}/create_report', json={
        'region': region,
        'report_type': report,
        'start_date': (end_date - datetime.timedelta(days=50)).strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    })

    r.raise_for_status()

    return r.json()['report_id']


# import datetime
#
# import requests
#
#
# def request_report(url: str, region: str, report: str, end_date: datetime.datetime | str | None) -> str:
#     if end_date is None:
#         end_date = datetime.datetime.now()
#     elif isinstance(end_date, str):
#         end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
#
#     r = requests.post(f'{url}/create_report', json={
#         'region': region,
#         'report_type': report,
#         'start_date': (end_date - datetime.timedelta(days=50)).strftime('%Y-%m-%d'),
#         'end_date': end_date.strftime('%Y-%m-%d')
#     })
#
#     r.raise_for_status()
#
#     return r.json()['report_id']
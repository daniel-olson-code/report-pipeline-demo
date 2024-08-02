import requests


def check_report_status(url: str, report_id: str) -> str:
    """
    Checks the status of a report.

    This function sends a GET request to the specified URL to check the status
    of a report identified by the given report ID.

    Args:
        url (str): The base URL of the API endpoint.
        report_id (str): The unique identifier of the report.

    Returns:
        str: The status of the report. Possible values are:
            - 'ready': The report is complete and ready for retrieval.
            - 'pending': The report is still being processed.
            - 'failed': The report generation has failed.

    Raises:
        requests.RequestException: If there's an error with the HTTP request.
        KeyError: If the 'status' key is not present in the JSON response.
        JSONDecodeError: If the response is not valid JSON.
        HTTPError: If the HTTP response indicates an error.

    Example:
        >>> status = check_report_status("https://api.example.com", "xxxx-xxxx-xxx-xxxx")
        >>> print(status)
        'ready'
    """
    r = requests.get(f'{url}/report_status/{report_id}')

    r.raise_for_status()

    return r.json()['status']


# import requests
#
#
# def check_report_status(url: str, report_id: str) -> str:
#     # Return 'ready', 'pending', or 'failed'
#     r = requests.get(f'{url}/report_status/{report_id}')
#     return r.json()['status']
import requests


def download_report(url: str, report_id: str) -> list[dict]:
    """
    Downloads a report from the specified URL.

    This function sends a GET request to the given URL to download a report
    identified by the provided report ID. It then extracts and returns the
    'data' field from the JSON response.

    Args:
        url (str): The base URL of the API endpoint.
        report_id (str): The unique identifier of the report to download.

    Returns:
        list[dict]: A list of dictionaries containing the report data.
            Each dictionary in the list represents a record or entry in the report.

    Raises:
        requests.HTTPError: If the HTTP request returns an unsuccessful status code.
        requests.RequestException: For other request-related errors.
        KeyError: If the 'data' key is not present in the JSON response.
        JSONDecodeError: If the response is not valid JSON.

    Example:
        >>> report_data = download_report("https://api.example.com", "12345")
        >>> print(len(report_data))
        10
        >>> print(report_data[0])
        {'id': 1, 'name': 'John Doe', 'value': 100}
    """
    r = requests.get(f'{url}/download_report/{report_id}')
    r.raise_for_status()

    return r.json()['data']


# import requests
#
#
# def download_report(url: str, report_id: str) -> list[dict]:
#     r = requests.get(f'{url}/download_report/{report_id}')
#     r.raise_for_status()
#
#     return r.json()['data']
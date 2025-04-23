import requests
import random
import time


def make_get_request(url: str, headers:dict = None, params: dict = None, rate_limit_count=0):
    print(f'Get request to {url} with params {params}')
    response = requests.get(url=url, headers=headers, params=params)
    status_code = response.status_code
    if status_code == 200:
        return response.json()

    elif response.status_code == 429 and rate_limit_count < 3:
        wait_time = random.uniform(1, rate_limit_count)
        print(f'Ratelimit reached, waiting {wait_time} then retrying with rate_limit_count {rate_limit_count}')
        time.sleep(wait_time)
        rate_limit_count += 1
        return make_get_request(
            url,
            headers=headers,
            params=params,
            rate_limit_count=rate_limit_count
        )

    else:
        print(f'Error getting data, status code: {status_code}, response: {response.text}')
        return None


def get_paginated_data(url: str, headers: dict = None, params: dict = None, limit: int = 100, max_offset: int = 1000):
    """
    Fetches paginated data from an API.

    :param url: API endpoint URL.
    :param headers: Optional HTTP headers.
    :param params: Optional query parameters (will be copied).
    :param limit: Number of records per request.
    :param max_offset: Maximum allowed offset to prevent infinite requests.
    :return: List of collected data from all pages.
    """
    if headers is None:
        headers = {}

    if params is None:
        params = {}

    data = []
    offset = 0

    while offset <= max_offset:
        params_copy = params.copy()  # Prevent mutation of original params
        params_copy['limit'] = limit
        params_copy['offset'] = offset

        response_json = make_get_request(url=url, headers=headers, params=params_copy)

        if not response_json or 'data' not in response_json:
            print(f'No data found in response for offset {offset}. Stopping pagination.')
            break

        data.extend(response_json.get('data', []))

        pagination_info = response_json.get('pagination', {})
        has_more_data = pagination_info.get('hasMore', False)

        if not has_more_data:
            break  # No more pages

        offset += limit
        wait_time = random.uniform(0, 2)
        print(f'Fetching more data... (offset: {offset}) Waiting {wait_time:.2f}s before next request.')
        time.sleep(wait_time)  # Rate-limiting delay

    return data

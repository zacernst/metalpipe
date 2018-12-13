'''
*****************************
Paginated GET Requests Module
*****************************

Classes and helper functions for handling paginated HTTP GET requests
'''

import os
import requests
import time
from nanostream.utils.helpers import SafeMap


additional_data_test = bool  # Function receiving `additional_data_key`; returns `True` if there is more pagination to request


class PaginatedHttpGetRequest:
    '''
    For handling requests in a semi-general way that require paging through
    lists of results and repeatedly making GET requests.
    '''

    def __init__(
        self, endpoint_template=None, additional_data_key=None,
        pagination_key=None, pagination_get_request_key=None,
            default_offset_value='', additional_data_test=bool):
        '''
        :ivar endpoint_template: (str) Template for endpoint URL, suitable
            for calling ``endpoint_template.format(**kwargs)``.
        :ivar additional_data_key: Key in JSON payload whose value indicates
            whether there are additional pages to request.
        :ivar pagination_key: Key in JSON payload where the current page
            (or next page) is indicated.
        :ivar pagination_get_request_key: Variable in URL GET request where
            we pass the offset for the next page.
        :ivar default_offset_value: Offset value for the first request. Usually
            this will be an empty string.
        :ivar additional_data_test: Function that is passed the value of
            ``additional_data_key``. It should return ``True`` if there are
            additional pages to request, ``False`` otherwise.
        '''
        self.endpoint_template = endpoint_template
        self.additional_data_key = additional_data_key
        self.pagination_key = pagination_key
        self.pagination_get_request_key = pagination_get_request_key
        self.default_offset_value = default_offset_value
        self.additional_data_test = additional_data_test

    def responses(self):
        '''
        Generator. Yields each response until empty.
        '''

        offset_set = set()

        get_request_parameters = {
            self.pagination_get_request_key: self.default_offset_value}
        endpoint_url = self.endpoint_template.format(**get_request_parameters)
        out = requests.get(endpoint_url).json()
        offset = out[self.pagination_key]
        offset_set.add(offset)

        while additional_data_test(out[self.additional_data_key]):
            get_request_parameters = {
                self.pagination_get_request_key: offset}
            endpoint_url = self.endpoint_template.format(**get_request_parameters)
            out = requests.get(endpoint_url).json()
            offset = out[self.pagination_key]
            yield out
            offset_set.add(offset)

if __name__ == '__main__':
    TWO_WEEKS_ENDPOINT_TEMPLATE = "https://api.hubapi.com/email/public/v1/events?hapikey={hubspot_api_key}&startTimestamp={start_timestamp}&endTimestamp={end_timestamp}&limit=5000&offset={offset}"

    get_request_parameters = {
        'hubspot_api_key': HUBSPOT_API_KEY,
        'start_timestamp': TWO_WEEKS_AGO,
        'end_timestamp': NOW}

    TWO_WEEKS_ENDPOINT = TWO_WEEKS_ENDPOINT_TEMPLATE.format_map(
        SafeMap(**get_request_parameters))

    r = PaginatedHttpGetRequest(
        pagination_get_request_key='offset',
        endpoint_template=TWO_WEEKS_ENDPOINT,
        additional_data_key='hasMore',
        pagination_key='offset')

    counter = 0
    for i in r.responses():
        counter += 1
        print(counter * 5000)

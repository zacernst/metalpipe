'''
Network nodes module
====================

Classes that deal with sending and receiving data across the interwebs.
'''

import requests
import json
import logging

from nanostream.node import NanoNode
from nanostream.utils.helpers import SafeMap

additional_data_test = bool


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
        :ivar additional_data_key: Key in JSON payload whose value
            indicates whether there are additional pages to request.
        :ivar pagination_key: Key in JSON payload where the current page
            (or next page) is indicated.
        :ivar pagination_get_request_key: Variable in URL GET request where
            we pass the offset for the next page.
        :ivar default_offset_value: Offset value for the first request.
            Usually this will be an empty string.
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
        endpoint_url = self.endpoint_template.format(
            **get_request_parameters)
        out = requests.get(endpoint_url)
        # out = out.json()
        out = json.loads(out.text)
        offset = out[self.pagination_key]
        offset_set.add(offset)

        while additional_data_test(out[self.additional_data_key]):
            get_request_parameters = {
                self.pagination_get_request_key: offset}
            endpoint_url = self.endpoint_template.format(
                **get_request_parameters)
            out = requests.get(endpoint_url).json()
            yield out
            try:
                offset = out[self.pagination_key]
                offset_set.add(offset)
            except KeyError:
                logging.info('No offset key. Assuming this is normal.')
                break


class HttpGetRequest(NanoNode):
    '''
    Node class for making simple GET requests.
    '''

    def __init__(
        self,
        url=None,
        endpoint_template='',
        endpoint_dict=None,
        json=True,
            **kwargs):
        self.endpoint_template = endpoint_template
        self.url = url
        self.endpoint_dict = endpoint_dict or {}
        self.json = json
        self.endpoint_dict.update(self.endpoint_dict)

        super(HttpGetRequest, self).__init__(**kwargs)

    def process_item(self):
        '''
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a
        dictionary with keys and values to be passed in the GET request.

        Three use-cases:
        1. Endpoint and parameters set initially and never changed.
        2. Endpoint and parameters set once at runtime
        3. Endpoint and parameters set by upstream messages
        '''

        # Hit the parameterized endpoint and yield back the results
        logging.debug('HttpGetRequest --->' + str(self.message))
        get_response = requests.get(
            self.endpoint_template.format(**(self.message or {})))
        output = get_response.json() if self.json else get_response.text
        yield output


class HttpGetRequestPaginator(NanoNode):
    '''
    Node class for HTTP API requests that require paging through sets of
    results.
    '''

    def __init__(
        self,
        endpoint_dict=None,
        json=True,
        pagination_get_request_key=None,
        endpoint_template=None,
        additional_data_key=None,
        pagination_key=None,
        pagination_template_key=None,
        default_offset_value='',
            **kwargs):
        self.pagination_get_request_key = pagination_get_request_key
        self.additional_data_key = additional_data_key
        self.pagination_key = pagination_key
        self.endpoint_dict = endpoint_dict or {}
        self.endpoint_template = endpoint_template or ''
        self.default_offset_value = default_offset_value

        self.endpoint_template = self.endpoint_template.format_map(
            SafeMap(**self.endpoint_dict))

        super(HttpGetRequestPaginator, self).__init__(**kwargs)

    def process_item(self):
        self.requestor = PaginatedHttpGetRequest(
            pagination_get_request_key=self.pagination_get_request_key,
            endpoint_template=self.endpoint_template.format_map(
                SafeMap(**(self.message or {}))),
            additional_data_key=self.additional_data_key,
            pagination_key=self.pagination_key,
            default_offset_value=self.default_offset_value)

        for i in self.requestor.responses():
            yield i

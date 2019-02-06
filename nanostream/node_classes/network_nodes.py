'''
Network nodes module
====================

Classes that deal with sending and receiving data across the interwebs.
'''

import requests
import json
import logging
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

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
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[ 500, 502, 503, 504 ])

        session.mount('http://', HTTPAdapter(max_retries=retries))

        get_request_parameters = {
            self.pagination_get_request_key: self.default_offset_value}
        endpoint_url = self.endpoint_template.format(
            **get_request_parameters)
        logging.info('paginator url: ' + endpoint_url)
        out = session.get(endpoint_url)
        # out = out.json()
        out = json.loads(out.text)
        offset = out.get(self.pagination_key, None)
        offset_set.add(offset)

        while self.additional_data_key in out and additional_data_test(out[self.additional_data_key]):
            yield out
            try:
                offset = out[self.pagination_key]
                offset_set.add(offset)
            except KeyError:
                logging.debug('No offset key. Assuming this is normal.')
                break
            get_request_parameters = {
                self.pagination_get_request_key: offset}
            endpoint_url = self.endpoint_template.format(
                **get_request_parameters)
            logging.debug('paginator url: ' + endpoint_url)
            try:
                response = session.get(endpoint_url)
                out = response.json()
            except:
                logging.warning('Error parsing. Assuming this is the end of the responses.')
                break


class HttpGetRequest(NanoNode):
    '''
    Node class for making simple GET requests.
    '''

    def __init__(
        self,
        url=None,
        endpoint_dict=None,
        json=True,
            **kwargs):
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
        formatted_endpoint = self.url.format_map(SafeMap(**(self.message or {})))
        try:
            formatted_endpoint = formatted_endpoint.format_map(SafeMap(**(self.endpoint_dict or {})))
        except Exception as err:
            print('formatted endpoint: ' + formatted_endpoint)
            raise Exception()
        logging.info('Http GET request: {endpoint}'.format(endpoint=formatted_endpoint))
        get_response = requests.get(formatted_endpoint)
        try:
            output = get_response.json()
        except JSONDecodeError:
            output = get_response.text
        logging.info(formatted_endpoint + ' GET RESPONSE: ' + str(output) + str(type(output)))
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
            logging.debug('paginator:' + str(self.name) + ' ' + str(i))
            if self.finished:
                break
            yield i

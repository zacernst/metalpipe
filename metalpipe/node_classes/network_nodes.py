"""
Network nodes module
====================

Classes that deal with sending and receiving data across the interwebs.
"""

import requests
import json
import time
import random
import logging
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from metalpipe.node import MetalNode
from metalpipe.utils.helpers import SafeMap

additional_data_test = bool


class PaginatedHttpGetRequest:
    """
    For handling requests in a semi-general way that require paging through
    lists of results and repeatedly making GET requests.
    """

    def __init__(
        self,
        endpoint_template=None,
        additional_data_key=None,
        pagination_key=None,
        pagination_get_request_key=None,
        protocol="http",
        retries=5,
        default_offset_value="",
        additional_data_test=bool,
        calling_node=None,
    ):
        """
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
        """
        self.endpoint_template = endpoint_template
        self.additional_data_key = additional_data_key
        self.pagination_key = pagination_key
        self.pagination_get_request_key = pagination_get_request_key
        self.protocol = protocol
        self.retries = retries
        self.default_offset_value = default_offset_value
        self.additional_data_test = additional_data_test

    def get_with_retry(self, url, error_on_none=True, **kwargs):
        error_counter = 0
        success = False
        hibernate = 1.0
        while error_counter < self.retries and not success:
            try:
                output = requests.get(url)
                if output is None:
                    logging.info(
                        "Request to {url} returned None".format(url=url)
                    )
                elif output.status_code >= 300:
                    logging.info(
                        "Request to {url} returned {code} status code".format(
                            url=url, code=str(output.status_code)
                        )
                    )
                else:
                    success = True
            except:
                error_counter += 1
                time.sleep(hibernate)
                hibernate *= 2
        if success:
            return output
        else:
            raise Exception("Failure for URL: {url}".format(url=url))

    def responses(self):
        """
        Generator. Yields each response until empty.
        """

        GET_ONLY = True

        offset_set = set()
        session = requests.Session()
        retries = Retry(
            total=self.retries,
            read=self.retries,
            connect=self.retries,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
        )

        session.mount(
            "{protocol}://".format(protocol=self.protocol),
            HTTPAdapter(max_retries=retries),
        )

        get_request_parameters = {
            self.pagination_get_request_key: self.default_offset_value
        }
        endpoint_url = self.endpoint_template.format(**get_request_parameters)
        logging.info("paginator url: " + endpoint_url)
        successful = False
        retry_counter = 0
        sleep_time = 1.0
        while not successful and retry_counter <= self.retries:
            try:
                out = requests.get(endpoint_url)
                successful = True
            except:
                logging.info(
                    "sleeping randomly... retry: {retry}".format(
                        retry=str(retry_counter)
                    )
                )
                retry_counter += 1
                time.sleep(sleep_time + (random.random() * 2))

        # Check if successful
        if not successful:
            logging.info(
                "Unsuccessful request to {url}".format(url=endpoint_url)
            )
            raise Exception("Unsuccessful GET request")

        # out = out.json()
        out = json.loads(out.text)
        offset = out.get(self.pagination_key, None)
        offset_set.add(offset)
        page_counter = 0
        while self.additional_data_key in out and additional_data_test(
            out[self.additional_data_key]
        ):
            try:
                offset = out[self.pagination_key]
                offset_set.add(offset)
            except KeyError:
                logging.debug("No offset key. Assuming this is normal.")
                break
            get_request_parameters = {self.pagination_get_request_key: offset}
            endpoint_url = self.endpoint_template.format(
                **get_request_parameters
            )
            logging.debug("paginator url: " + endpoint_url)
            try:
                # response = session.get(endpoint_url)

                if GET_ONLY:
                    response = self.get_with_retry(endpoint_url)
                else:
                    response = session.get(endpoint_url)

                out = response.json()
            except:
                logging.warning(
                    "Error parsing. Assuming this is the "
                    "end of the responses."
                )
                break
            yield out


class HttpGetRequest(MetalNode):
    """
    Node class for making simple GET requests.
    """

    def __init__(
        self,
        endpoint_template=None,
        endpoint_dict=None,
        protocol="http",
        retries=5,
        json=True,
        **kwargs
    ):
        self.endpoint_template = endpoint_template
        self.endpoint_dict = endpoint_dict or {}
        self.protocol = protocol
        self.json = json
        self.retries = retries
        self.endpoint_dict.update(self.endpoint_dict)

        super(HttpGetRequest, self).__init__(**kwargs)

    def process_item(self):
        """
        The input to this function will be a dictionary-like object with
        parameters to be substituted into the endpoint string and a
        dictionary with keys and values to be passed in the GET request.

        Three use-cases:
        1. Endpoint and parameters set initially and never changed.
        2. Endpoint and parameters set once at runtime
        3. Endpoint and parameters set by upstream messages
        """

        # Hit the parameterized endpoint and yield back the results
        formatted_endpoint = self.endpoint_template.format_map(
            SafeMap(**(self.message or {}))
        )
        try:
            formatted_endpoint = formatted_endpoint.format_map(
                SafeMap(**(self.endpoint_dict or {}))
            )
        except Exception:
            logging.error("formatted endpoint: " + formatted_endpoint)
            raise Exception()
        logging.info(
            "Http GET request: {endpoint}".format(endpoint=formatted_endpoint)
        )

        session = requests.Session()
        retries = Retry(
            total=self.retries,
            backoff_factor=0.1,
            status_forcelist=[500, 502, 503, 504],
        )
        session.mount(
            "{protocol}://".format(protocol=self.protocol),
            HTTPAdapter(max_retries=retries),
        )

        get_response = session.get(formatted_endpoint)
        try:
            output = get_response.json()
        except json.JSONDecodeError:
            output = get_response.text
        logging.debug(
            formatted_endpoint
            + " GET RESPONSE: "
            + str(output)
            + str(type(output))
        )
        yield output


class HttpGetRequestPaginator(MetalNode):
    """
    Node class for HTTP API requests that require paging through sets of
    results.
    """

    def __init__(
        self,
        endpoint_dict=None,
        json=True,
        pagination_get_request_key=None,
        endpoint_template=None,
        additional_data_key=None,
        pagination_key=None,
        pagination_template_key=None,
        default_offset_value="",
        **kwargs
    ):
        self.pagination_get_request_key = pagination_get_request_key
        self.additional_data_key = additional_data_key
        self.pagination_key = pagination_key
        self.endpoint_dict = endpoint_dict or {}
        self.endpoint_template = endpoint_template or ""
        self.default_offset_value = default_offset_value

        self.endpoint_template = self.endpoint_template.format_map(
            SafeMap(**self.endpoint_dict)
        )

        super(HttpGetRequestPaginator, self).__init__(**kwargs)

    def process_item(self):
        self.requestor = PaginatedHttpGetRequest(
            pagination_get_request_key=self.pagination_get_request_key,
            calling_node=self,
            endpoint_template=self.endpoint_template.format_map(
                SafeMap(**(self.message or {}))
            ),
            additional_data_key=self.additional_data_key,
            pagination_key=self.pagination_key,
            default_offset_value=self.default_offset_value,
        )

        for i in self.requestor.responses():
            logging.debug(
                "paginator GET request:"
                + str(self.name)
                + " "
                + str(self.requestor.endpoint_template)
            )
            logging.debug(
                "::".join([self.name, "events", str(len(i.get("events", "")))])
            )
            yield i
            if self.finished:
                break

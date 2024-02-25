# ./utils/__init__.py

# < Import commonly used functions/modules from submodules ------------

from .constants import *
from .functions import *
from .objects import *
from .dates import *
from .kafka import *
from .logs import *
from .api import *
from .sql import *

import argparse
from functools import partial
from typing import *


# < Initialize the Module ---------------------------------------------
# Define some common applications of the n_days_before function
now = partial(n_days_before, days=0, midnight=False)
today = partial(n_days_before, days=0, midnight=True)
yesterday = partial(n_days_before, days=1, midnight=True)
two_days_ago = partial(n_days_before, days=2, midnight=True)

@handle("Testing Connection to the endpoints.", strict=False, do_raise=True, max_retries=5)
def test_connection(endpoint: ApiEndpoint) -> bool:
    """
    Tests the connection to the ConnectivityEndpoint.

    Returns
    -------
        bool: True if connection is successful and the expected response is received.

    Raises
    ------
        ConnectionError: If the connection is unsuccessful or the response is unexpected.
    """

    response = endpoint.GET()
    code = response.status_code
    if code != 200:
        raise ConnectionError(
            f"Can't connect to the [{endpoint.name}]: {code}")

    printlog(f"Connection to [{endpoint.name}] Successful: {code}", "info")
    return code == 200


@trace("Testing Connection to the endpoints.")
@handle("Testing Connection to the endpoints.", strict=False, do_raise=True, max_retries=3)
def test_endpoints(*endpoints: List[ApiEndpoint], debug=False):
    # if debug:
    #     printlog("TESTING || Skipping Connectivity Checks", "warning")
    #     return True
    # else:
        return all(test_connection(ep) for ep in endpoints)
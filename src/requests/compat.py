"""
requests.compat
~~~~~~~~~~~~~~~

This module previously handled import compatibility issues
between Python 2 and Python 3. It remains for backwards
compatibility until the next major version.
"""

import importlib
import sys

# -------
# urllib3
# -------
from urllib3 import __version__ as urllib3_version

# Detect which major version of urllib3 is being used.
try:
    is_urllib3_1 = int(urllib3_version.split(".")[0]) == 1
except (TypeError, AttributeError):
    # If we can't discern a version, prefer old functionality.
    is_urllib3_1 = True

# -------------------
# Character Detection
# -------------------


def _resolve_char_detection():
    """Find supported character detection libraries."""
    chardet = None
    for lib in ("chardet", "charset_normalizer"):
        if chardet is None:
            try:
                chardet = importlib.import_module(lib)
            except ImportError:
                pass
    return chardet


chardet = _resolve_char_detection()

# -------
# Pythons
# -------

# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3

# json/simplejson module import resolution
has_simplejson = False
try:
    import simplejson as json

    has_simplejson = True
except ImportError:
    pass

if has_simplejson:
    pass
else:
    pass

# Keep OrderedDict for backwards compatibility.

# --------------
# Legacy Imports
# --------------

builtin_str = str
str = str
bytes = bytes
basestring = (str, bytes)
numeric_types = (int, float)
integer_types = (int,)

try:
    from json import JSONDecodeError
except ImportError:
    class JSONDecodeError(ValueError):
        pass

try:
    from collections.abc import Callable, Mapping, MutableMapping
except ImportError:
    from collections import Callable, Mapping, MutableMapping

try:
    from urllib.request import getproxies, proxy_bypass, proxy_bypass_environment, getproxies_environment, parse_http_list
except ImportError:
    from urllib import getproxies, proxy_bypass, proxy_bypass_environment, getproxies_environment
    from urllib2 import parse_http_list

try:
    from urllib.parse import urlparse, urlunparse, urljoin, urlsplit, urlunsplit, quote, unquote, quote_plus, unquote_plus, urldefrag, urlencode, parse_qsl
except ImportError:
    from urlparse import urlparse, urlunparse, urljoin, urlsplit, urldefrag
    from urllib import quote, unquote, quote_plus, unquote_plus, urlencode, parse_qsl

try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

try:
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import HTTPError

try:
    import cookielib
except ImportError:
    from http import cookiejar as cookielib

try:
    from http.cookies import Morsel
except ImportError:
    from Cookie import Morsel

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

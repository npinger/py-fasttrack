"""
Copyright 2012 Nathan Pinger <npinger@ycharts.com>
Thanks to Numan Sachwani <numan@7Geese.com> for the
original release of py-analytics.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
from fasttrack.utils import import_string

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('fasttrack').version
except Exception, e:
    VERSION = 'unknown'


def create_analytic_backend(settings):
    """
    Creates a new Analytics backend from the settings

    :param settings: Dictionary of settings for the analytics backend
    :returns: A backend object implementing the analytics api

    >>>
    >>> analytics = create_analytic({
    >>>     'backend': 'fasttrack.backends.redis_backend.Redis',
    >>>     'settings': {
    >>>         'defaults': {
    >>>             'host': 'localhost',
    >>>             'port': 6379,
    >>>             'db': 0,
    >>>         },
    >>>     },
    >>> })
    """
    backend = settings.get('backend')
    if isinstance(backend, basestring):
        backend = import_string(backend)
    elif backend:
        backend = backend
    else:
        raise KeyError('backend')

    return backend(settings.get("settings", {}))

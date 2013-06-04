
py-fasttrack
============

This is a serious redesign of numan/py-analytics v0.5.3.  The main architectural change is to remove `nydus` from the projects core as `nydus` threading was impeding performance.  The project has been renamed because there will be no attempt to merge back into the original branch.

py-fasttrack is a library designed to make it easy to provide analytics as part of any project.

The project's goal is to make it easy to store and retrieve analytics data. It does not provide
any means to visualize this data.

Currently, only ``Redis`` is supported for storing data.

Requirements
------------

Required
~~~~~~~~

Requirements **should** be handled by setuptools, but if they are not, you will need the following Python packages:

* redis
* dateutil

Optional
~~~~~~~~

* hiredis

fasttrack.create_analytic_backend
----------------------------------

Creates an analytics object that allows to to store and retrieve metrics::

    >>> from fasttrack import create_analytic_backend
    >>>
    >>> tracker = create_analytic_backend({
    >>>     'backend': 'fasttrack.backends.redis_backend.Redis',
    >>>     'settings': {
    >>>         'defaults': {
    >>>             'host': 'localhost',
    >>>             'port': 6379,
    >>>             'db': 0,
    >>>         },
    >>>     },
    >>> })

Internally, the ``Redis`` analytics backend uses ``nydus`` to distribute your metrics data over your cluster of redis instances.

There are two required arguements:

* ``backend``: full path to the backend class, which should extend fasttrack.backends.base.BaseAnalyticsBackend
* ``settings``: settings required to initialize the backend. For the ``Redis`` backend, this is a list of hosts in your redis cluster.

Example Usage
-------------

::

    from fasttrack import create_analytic_backend
    import datetime

    tracker = create_analytic_backend({
        "backend": "fasttrack.backends.redis.Redis",
        "settings": {
            "default": {
                'host': 'localhost',
                'port': 6379,
                'db': 0,
            }
        },
    })

    year_ago = datetime.date.today() - datetime.timedelta(days=265)

    #create some analytics data
    # NOTE THAT ALL NUMBERS IN INITIAL ARGUMENTS ARE ASSUMED TO BE
    # UNIQUE USER IDS
    tracker.track_metric("1234", "comment", year_ago)
    tracker.track_metric("1234", "comment", year_ago, inc_amt=3)
    #we can even track multiple metrics at the same time for a particular user
    tracker.track_metric("1234", ["comments", "likes"], year_ago)
    #or track the same metric for multiple users (or a combination or both)
    tracker.track_metric(["1234", "4567"], "comment", year_ago)

    #retrieve analytics data:
    tracker.get_metric_by_day("1234", "comment", year_ago, limit=20)
    tracker.get_metric_by_week("1234", "comment", year_ago, limit=10)
    tracker.get_metric_by_month("1234", "comment", year_ago, limit=6)

    #create a counter
    tracker.track_count("1245", "login")
    tracker.track_count("1245", "login", inc_amt=3)

    #retrieve multiple metrics at the same time
    #group_by is one of ``month``, ``week`` or ``day``
    tracker.get_metrics([("1234", "login",), ("4567", "login",)], year_ago, group_by="day")
    >> [....]

    #retrieve a count
    tracker.get_count("1245", "login")

    #retrieve a count between 2 dates
    tracker.get_count("1245", "login", start_date=datetime.date(month=1, day=5, year=2011), end_date=datetime.date(month=5, day=15, year=2011))

    #retrieve counts
    tracker.get_counts([("1245", "login",), ("1245", "logout",)])


BACKWARDS INCOMPATIBLE CHANGES
-------------------------------



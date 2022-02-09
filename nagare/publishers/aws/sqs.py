# --
# Copyright (c) 2008-2022 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""The AWS SQS publisher"""

from functools import partial

from nagare.server import publisher


class Publisher(publisher.Publisher):
    """The AWS SQS publisher"""

    CONFIG_SPEC = dict(
        publisher.Publisher.CONFIG_SPEC,
        queue='string(help="name of the queue to listen to")'
    )
    has_multi_threads = True

    def __init__(self, name, dist, services_service, **conf):
        services_service(super(Publisher, self).__init__, name, dist, **conf)
        self.queue = None

    def generate_banner(self):
        banner = super(Publisher, self).generate_banner()
        return banner + ' on queue `{}`'.format(str(self.queue.name))

    def start_handle_request(self, app, services, msg):
        try:
            super(Publisher, self).start_handle_request(app, services, queue=self.queue, msg=msg)
        except Exception:
            pass

    def _serve(self, app, queue, services_service, **conf):
        self.queue = services_service[queue]
        super(Publisher, self)._serve(app)

        self.queue.start_consuming(partial(self.start_handle_request, app, services_service))

        return 0

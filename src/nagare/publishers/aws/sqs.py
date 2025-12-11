# --
# Copyright (c) 2014-2025 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""The AWS SQS publisher."""

from functools import partial

from nagare.server import publisher


class Publisher(publisher.Publisher):
    """The AWS SQS publisher."""

    CONFIG_SPEC = publisher.Publisher.CONFIG_SPEC | {'queue': 'string(help="name of the queue to listen to")'}
    has_multi_threads = True

    def __init__(self, name, dist, services_service, **conf):
        services_service(super().__init__, name, dist, **conf)
        self.queue = None

    def generate_banner(self):
        banner = super().generate_banner()
        return banner + ' on queue `{}`'.format(str(self.queue.name))

    def start_handle_request(self, app, services, msg):
        try:
            super().start_handle_request(app, services, queue=self.queue, msg=msg)
        except Exception:  # noqa: S110
            pass

    def _serve(self, app, queue, services_service, **conf):
        self.queue = services_service[queue]
        super()._serve(app)

        self.queue.start_consuming(partial(self.start_handle_request, app, services_service))

        return 0

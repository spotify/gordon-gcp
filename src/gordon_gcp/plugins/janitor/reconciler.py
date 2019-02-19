# -*- coding: utf-8 -*-
#
# Copyright 2018 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Module to compare desired record sets produced from a Resource Authority
(i.e. ``GCEInstanceAuthority`` for Google Compute Engine) and actual
record sets from Google Cloud DNS, then publish corrective messages to
the internal ``changes_channel`` if there are differences.

This client makes use of the asynchronous DNS client as defined in
:class:`.GDNSClient`, and therefore must use service account/JWT
authentication (for now).

See :doc:`config-janitor` for the required Google DNS configuration.

.. attention::

    This reconciler client is an internal module for the core janitor
    logic. No other use cases are expected.

To use:

.. code-block:: python

    import asyncio
    import gordon_gcp

    config = {
        'keyfile': '/path/to/keyfile.json',
        'project': 'a-dns-project'
    }
    rrset_chnl = asyncio.Queue()
    changes_chnl = asyncio.Queue()

    reconciler = gordon_gcp.GDNSReconciler(
        config, rrset_chnl, changes_chnl)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(reconciler.start())
    finally:
        loop.close()
"""

import asyncio
import collections
import logging

import zope.interface
from gordon_janitor import interfaces

from gordon_gcp import exceptions
from gordon_gcp.clients import auth
from gordon_gcp.clients import gdns


__all__ = ('GDNSReconciler',)


class ResourceRecordSet:
    """DNS Resource Record Set.

    Args:
        name (str): Name/label.
        kind (str): ID for what kind of GCP resource this is. For example
            'dns#resourceRecordSet'
        type (str): Record type (see `Google's supported records
            <https://cloud.google.com/dns/overview#supported_dns_record_
            types>`_ for valid types).
        rrdatas (iter(str)): Record data according to RFC 1034§3.6.1 and
            RFC 1035§5.
        ttl (int): (optional) Number of seconds that the record set can
            be cached by resolvers. Defaults to 300.
        source (str): (optional) Source of the record set, not considered
            for equality comparisons.
    """
    def __init__(self, name, type, rrdatas, kind='dns#resourceRecordSet',
                 ttl=300, source=None):
        self.name = name
        self.type = type
        self.rrdatas = tuple(rrdatas)
        self.kind = kind
        self.ttl = ttl
        self.source = source

    def __hash__(self):
        return hash((self.name, self.type, self.rrdatas, self.kind, self.ttl))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return repr(vars(self))


class GDNSReconcilerBuilder:
    """Build and configure a :class:`GDNSReconciler` object.

    Args:
        config (dict): Google Cloud DNS-related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        rrset_channel (asyncio.Queue): queue from which to consume
            record set messages to validate.
        changes_channel (asyncio.Queue): queue to publish message to
            make corrections to Cloud DNS.
    """
    def __init__(self, config, metrics, rrset_channel, changes_channel,
                 **kwargs):
        self.config = config
        self.metrics = metrics
        self.rrset_channel = rrset_channel
        self.changes_channel = changes_channel
        self.kwargs = kwargs

    def _validate_config(self):
        # req keys: keyfile, project, topic
        # TODO (lynn): keyfile won't be required once we support other
        #              auth methods
        if not self.config.get('keyfile'):
            msg = ('The path to a Service Account JSON keyfile is required to '
                   'authenticate for Google Cloud Pub/Sub.')
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)
        if not self.config.get('project'):
            msg = 'The GCP project where Cloud DNS is located is required.'
            logging.error(msg)
            raise exceptions.GCPConfigError(msg)

    def _init_auth(self):
        return auth.GAuthClient(
            keyfile=self.config['keyfile'], scopes=self.config.get('scopes'))

    def _init_client(self, auth_client):
        kwargs = {
            'project': self.config['project'],
            'api_version': self.config.get('api_version', 'v1'),
            'auth_client': auth_client,
            'default_zone_prefix': self.config.get('default_zone_prefix', '')
        }
        return gdns.GDNSClient(**kwargs)

    def build_reconciler(self):
        self._validate_config()
        auth_client = self._init_auth()
        dns_client = self._init_client(auth_client)
        return GDNSReconciler(
            self.config, self.metrics, dns_client, self.rrset_channel,
            self.changes_channel, **self.kwargs)


@zope.interface.implementer(interfaces.IReconciler)
class GDNSReconciler:
    """Validate current records in DNS against desired source of truth.

    :class:`.GDNSReconciler` will create a change message for the
    configured publisher client plugin to consume if there is a
    discrepancy between records in Google Cloud DNS and the desired
    state.

    Once validation is done, the Reconciler will emit a ``None`` message
    to the ``changes_channel`` queue, signalling a Publisher client
    (e.g. :class:`.GPubsubPublisher`) to publish the
    message to a pub/sub to which `Gordon <https://github.com/spotify/
    gordon>`_ subscribes.

    Args:
        config (dict): Google Cloud DNS-related configuration.
        metrics (obj): :interface:`IMetricRelay` implementation.
        dns_client (.GDNSClient): Client to interact with Google Cloud
            DNS API.
        rrset_channel (asyncio.Queue): Queue from which to consume
            record set messages to validate.
        changes_channel (asyncio.Queue): Queue to publish message to
            make corrections to Cloud DNS.
    """

    _ASYNC_METHODS = ['publish_change_messages', 'validate_rrsets_by_zone']

    def __init__(self, config, metrics, dns_client, rrset_channel=None,
                 changes_channel=None, **kw):
        self.metrics = metrics
        self.dns_client = dns_client
        self.rrset_channel = rrset_channel
        self.changes_channel = changes_channel
        self.cleanup_timeout = config.get('cleanup_timeout', 60)

    async def cleanup(self):
        """Clean up & notify :obj:`changes_channel` of no more messages.

        This method collects all tasks that this particular class
        initiated, and will cancel them if they don't complete within
        the configured timeout period.

        Once all tasks are done, ``None`` is added to the
        :obj:`changes_channel` to signify that it has no more work to
        process. Then the HTTP session attached to the :obj:`dns_client`
        is properly closed.
        """
        all_tasks = asyncio.Task.all_tasks()
        tasks_to_clear = [
            t for t in all_tasks if t._coro.__name__ in self._ASYNC_METHODS
        ]
        sleep_for = 0.5  # half a second
        iterations = self.cleanup_timeout / sleep_for

        while iterations:
            tasks_to_clear = [t for t in tasks_to_clear if not t.done()]
            if not tasks_to_clear:
                break

            await asyncio.sleep(sleep_for)
            iterations -= 1

        # give up on waiting for tasks to complete
        if tasks_to_clear:
            msg = (f'The following tasks did not complete in time and are '
                   f'being cancelled: {tasks_to_clear}')
            logging.warning(msg)
            for task in tasks_to_clear:
                task.cancel()

        await self.changes_channel.put(None)
        await self.dns_client._session.close()

        msg = ('Reconciliation of desired records against actual records in '
               'Google DNS is complete.')
        logging.info(msg)

    async def publish_change_messages(self, desired_rrsets, action='additions'):
        """Publish change messages to the :obj:`changes_channel`.

        NOTE: Only `'additions'` are currently supported. `'deletions'`
        may be supported in the future.

        Args:
            desired_rrsets (list(ResourceRecordSet)): Desired record
                sets that are not in Google Cloud DNS.
            action (str): (optional) action for these corrective
                messages. Defaults to ``'additions'``.
        """
        source_count = collections.defaultdict(int)
        for rrset in desired_rrsets:
            rrset = vars(rrset)

            # count and remove source before sending message to channel
            source = rrset.pop('source', None)
            source_count[source] += 1

            msg = {
                'resourceRecords': rrset,
                'action': action
            }
            logging.debug(f'Creating the following change message: {msg}')
            await self.changes_channel.put(msg)

        context = {'plugin': 'reconciler', 'action': action}
        for source, count in source_count.items():
            context['source'] = source if source else 'unknown'
            await self.metrics.set('rrsets-handled', count, context=context)

        logging.info(
            f'Created {len(desired_rrsets)} change messages for {action}.')

    def _parse_rrset_message(self, message):
        # assert that keys 'zone' and 'rrsets' are present, and return
        # values for each
        try:
            zone = message['zone']
        except KeyError:
            msg = (f'No zone was defined in the given message: {message}.')
            logging.error(msg)
            raise exceptions.GCPGordonJanitorError(msg)
        try:
            rrsets = message['rrsets']
        except KeyError:
            msg = (f'No resource record sets were defined in given message: '
                   f'{message}.')
            logging.error(msg)
            raise exceptions.GCPGordonJanitorError(msg)

        return zone, rrsets

    @staticmethod
    def create_rrset_set(zone, rrsets):
        """Create a set of ResourceRecordSets excluding SOA and zone's NS.

        Args:
            zone (str): zone of the rrsets, for NS record exclusion.
            rrsets (list(dict)): collection of dict representation of RRSets.

        Returns:
            set of :class:`ResourceRecordSet`
        """
        rrset_set = set()
        for rrset in rrsets:
            name = rrset['name']
            typeStr = rrset['type']
            if typeStr == 'SOA' or (typeStr == 'NS' and name == zone):
                continue
            rrset_set.add(ResourceRecordSet(**rrset))
        return rrset_set

    async def validate_rrsets_by_zone(self, zone, rrsets):
        """Given a zone, validate current versus desired rrsets.

        Returns lists of missing rrsets (in desired but not in current) and
        extra rrsets (in current but not in desired).  Extra rrsets that are the
        result of updates in the desired list will not be returned, and root
        SOA/NS comparisons are skipped.

        Args:
            zone (str): zone to query Google Cloud DNS API.
            rrsets (list): desired record sets to which to compare the
                Cloud DNS API's response.

        Returns:
            tuple[set(rrset), set(rrset)]: The missing and extra rrset sets.
        """

        desired_rrsets = self.create_rrset_set(zone, rrsets)
        actual_rrsets = self.create_rrset_set(
            zone, await self.dns_client.get_records_for_zone(zone))

        missing_rrsets = desired_rrsets - actual_rrsets
        # TODO: This should eventually also emit an actual metric.
        msg = (f'[{zone}] Processed {len(actual_rrsets)} rrset messages '
               f'and found {len(missing_rrsets)} missing rrsets.')
        logging.info(msg)

        # Assemble the deletions, but not if there are no desired rrsets,
        # which could mean the authority failed.  Note: if the authority
        # doesn't emit anything for a zone at all, we won't even get this far.
        if not desired_rrsets:
            msg = (f'[{zone}] No desired rrsets for zone; refusing to delete '
                   'all records.')
            logging.info(msg)
            extra_rrsets = set()
        else:
            extra_rrsets = actual_rrsets - desired_rrsets
            msg = (f'[{zone}] Processed {len(actual_rrsets)} rrset messages '
                   f'and found {len(extra_rrsets)} extra rrsets.')
            logging.info(msg)

        return missing_rrsets, extra_rrsets

    async def run(self):
        """Publish necessary DNS changes to the :obj:`changes_channel`.

        Consumes zone/rrset-list messages from :obj:`rrset_channel`, compares
        them to the current records, and publishes the changes.  Once ``None``
        is received from the channel, emits a final ``None`` message to the
        :obj:`changes_channel`.
        """
        context = {'plugin': 'reconciler'}
        timer = self.metrics.timer('plugin-runtime', context=context)
        await timer.start()
        while True:
            desired_rrset = await self.rrset_channel.get()
            if desired_rrset is None:
                break
            try:
                zone, raw_rrsets = self._parse_rrset_message(desired_rrset)
                missing_rrsets, extra_rrsets = (
                    await self.validate_rrsets_by_zone(zone, raw_rrsets))

                # In the case of an update, we'll end up with both an addition
                # and a deletion, so send deletions first.
                await self.publish_change_messages(
                    extra_rrsets, action='deletions')
                await self.publish_change_messages(
                    missing_rrsets, action='additions')
            except exceptions.GCPGordonJanitorError as e:
                msg = f'Dropping message {desired_rrset}: {e}'
                logging.error(msg, exc_info=e)

        await self.cleanup()
        await timer.stop()

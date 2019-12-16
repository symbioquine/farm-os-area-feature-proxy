#!/bin/env python3

from twisted.internet import defer

from tx_drupal_rest_ws_client import TxDrupalRestWsClient

class TxFarmOsClient(object):
    def __init__(self, drupal_client):
        self.drupal_client = drupal_client
        self.area = TxFarmOsAreaClient(drupal_client)

    @classmethod
    def create(cls, farm_os_url, user, password, reactor=None, cookie_jar=None, user_agent="TxFarmOsClient"):
        drupal_client = TxDrupalRestWsClient.create(drupal_url=farm_os_url, user=user, password=password, reactor=reactor, cookie_jar=cookie_jar, user_agent=user_agent)

        return cls(drupal_client)

class TxFarmOsAreaClient(object):
    def __init__(self, drupal_client):
        self._drupal_client = drupal_client

        self._area_vocabulary_id = None
        self._area_vocabulary_id_lock = defer.DeferredLock()

    @defer.inlineCallbacks
    def get_by_id(self, area_id, validate_type=True):
        entity = yield self._drupal_client.get_entity(entity_type='taxonomy_term', entity_id=area_id)

        if validate_type:
            vid = yield self._get_area_vocabulary_id()

            if vid != entity.get('vocabulary', {}).get('id', None):
                raise Exception("Entity is not a farm area: " + entity)

        return entity

    def get_all(self):
        return self._drupal_client.get_all_entities('taxonomy_term', {'bundle': 'farm_areas'})

    @defer.inlineCallbacks
    def create(self, record):
        vid = yield self._get_area_vocabulary_id()

        entity_record = {}
        entity_record.update(record)
        entity_record['vocabulary'] = vid

        entity = yield self._drupal_client.create_entity('taxonomy_term', entity_record)

        return entity

    @defer.inlineCallbacks
    def update(self, area_id, record):
        yield self._drupal_client.update_entity('taxonomy_term', area_id, record)

    @defer.inlineCallbacks
    def delete(self, area_id):
        yield self._drupal_client.delete_entity('taxonomy_term', area_id)

    @defer.inlineCallbacks
    def _get_area_vocabulary_id(self):
        # Make sure this fn is always a generator
        yield defer.succeed(True)

        if not self._area_vocabulary_id is None:
            return self._area_vocabulary_id

        yield self._area_vocabulary_id_lock.acquire()
        try:
            if not self._area_vocabulary_id is None:
                return self._area_vocabulary_id

            vocabularies = yield self._drupal_client.get_entities('taxonomy_vocabulary', {'machine_name': 'farm_areas'})

            farm_area_vocabulary = list(vocabularies)[0]

            self._area_vocabulary_id = farm_area_vocabulary['vid']

            return self._area_vocabulary_id
        finally:
            self._area_vocabulary_id_lock.release()

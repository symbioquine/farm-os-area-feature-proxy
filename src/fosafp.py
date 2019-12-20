#!/bin/env python3

import sys, argparse, logging
from functools import partial, lru_cache

from twisted.application import service, strports
from twisted.python import log
from twisted.web import server
from twisted.internet import reactor, defer, task

from cachetools import cached, TTLCache

from osgeo import ogr, osr

from tx_farm_os_client import TxFarmOsClient
from wfs_resource import WfsResource, LayerDefinition, FeatureField, Feature, TransactionOutcome, CommitOutcomeItem


AREAS_CACHE_SECONDS = 60
CLIENT_INSTANCE_CACHE_SIZE = 32
TRANSACTION_COMMIT_PARALLELISM = 16


class _AllAreasCacheCell(object):
    def __init__(self, _ignored):
        self.lock = defer.DeferredLock()
        self.value = None

class FarmOsProxyFeatureServer(object):
    name = "FarmOsProxyFeatureServer"

    def __init__(self, farm_os_url):
        farm_os_client_creation_lock = defer.DeferredLock()

        self._create_farm_os_client = partial(farm_os_client_creation_lock.run,
                                              lru_cache(maxsize=CLIENT_INSTANCE_CACHE_SIZE)(partial(TxFarmOsClient.create, farm_os_url, user_agent="FarmOsAreaFeatureProxy")))

        all_areas_cache_lock = defer.DeferredLock()
        self._get_all_areas_cache_cell = partial(all_areas_cache_lock.run,
                                                 cached(cache=TTLCache(maxsize=CLIENT_INSTANCE_CACHE_SIZE, ttl=AREAS_CACHE_SECONDS))(_AllAreasCacheCell))

    def layer_definitions(self, request):
        return [
            LayerDefinition(
                name='farm_os_features_' + layer_type,
                title="FarmOS {} features".format(layer_type.replace('_', ' ')),
                default_srs='EPSG:4326',
                geometry_type="{}PropertyType".format(''.join(map(str.capitalize, layer_type.split('_')))),
                operations={'Query', 'Insert', 'Update', 'Delete'},
                fields=(
                    FeatureField(name=field_name, field_type='string', required=(field_name != 'description')) for field_name in ('name', 'area_type', 'description')
                ),
                ext={'geojson_type': layer_type.replace('_', '')}
            ) for layer_type in ('point', 'polygon', 'line_string')
        ]

    @defer.inlineCallbacks
    def get_all_features(self, layer_def, request):
        farm_os_client = yield self._create_farm_os_client(request.getUser(), request.getPassword())

        geojson_area_features = yield self._cached_get_all_areas(farm_os_client)

        return self._to_type_filtered_layer_features(layer_def, geojson_area_features)

    def _to_type_filtered_layer_features(self, layer_def, geojson_area_features):
        def to_layer_feature(geojson_area_feature):
            geofield = geojson_area_feature.get('geofield', [])

            if len(geofield) != 1:
                return None

            if geofield[0]['geo_type'] != layer_def.ext.geojson_type:
                return None

            area_id = geojson_area_feature.get('tid')

            feature_id = layer_def.name + '.' + area_id

            def extract_field_items():
                for field in layer_def.fields:
                    v = geojson_area_feature.get(field.name, None)

                    if v is None:
                        continue

                    yield (field.name, v)

            field_data = {field_name: field_value for (field_name, field_value) in extract_field_items()}

            geometry = ogr.CreateGeometryFromWkt(geofield[0].get('geom'))

            srs = osr.SpatialReference()
            srs.SetFromUserInput(layer_def.default_srs)

            geometry.AssignSpatialReference( srs )

            return Feature(feature_id=feature_id, geometry=geometry, field_data=field_data)

        layer_features = map(to_layer_feature, geojson_area_features)

        return filter(lambda feature: feature is not None, layer_features)

    @defer.inlineCallbacks
    def commit_transaction(self, transaction, request):
        farm_os_client = yield self._create_farm_os_client(request.getUser(), request.getPassword())

        inserted_features = []
        updated_features = []
        deleted_features = []
        transaction_failures = []

        def work_iter():
            for feature_to_insert in transaction.features_to_insert:
                @defer.inlineCallbacks
                def insert_feature_work():
                    record = {}
                    record.update(feature_to_insert.field_data)
                    record['geofield'] = [
                        {
                            "geom": feature_to_insert.geometry.ExportToWkt()
                        }
                    ]

                    try:
                        response = yield farm_os_client.area.create(record)

                        feature_id = feature_to_insert.layer_def.name + '.' + response.get('id')

                        inserted_features.append(CommitOutcomeItem(data=feature_id, layer_def=feature_to_insert.layer_def, handle=feature_to_insert.handle))
                    except:
                        formatted_exception = logging.traceback.format_exc()
                        logging.error(formatted_exception)
                        transaction_failures.append(CommitOutcomeItem(data=formatted_exception, layer_def=feature_to_insert.layer_def, handle=feature_to_insert.handle))

                yield insert_feature_work()

            for feature_to_update in transaction.features_to_update:
                @defer.inlineCallbacks
                def update_feature_work():
                    feature_id = feature_to_update.feature_id

                    numeric_feature_id = feature_id.rsplit('.', 1)[1]

                    record = {}
                    record.update(feature_to_update.field_data)

                    if not feature_to_update.geometry is None:
                        record['geofield'] = [
                            {
                                "geom": feature_to_update.geometry.ExportToWkt()
                            }
                        ]

                    try:
                        yield farm_os_client.area.update(numeric_feature_id, record)

                        updated_features.append(CommitOutcomeItem(data=feature_id, layer_def=feature_to_update.layer_def, handle=feature_to_update.handle))
                    except:
                        formatted_exception = logging.traceback.format_exc()
                        logging.error(formatted_exception)
                        transaction_failures.append(CommitOutcomeItem(data=formatted_exception, layer_def=feature_to_update.layer_def, handle=feature_to_update.handle))

                yield update_feature_work()

            for feature_to_delete in transaction.features_to_delete:
                @defer.inlineCallbacks
                def delete_feature_work():
                    feature_id = feature_to_delete.feature_id

                    numeric_feature_id = feature_id.rsplit('.', 1)[1]

                    try:
                        yield farm_os_client.area.delete(numeric_feature_id)
                        deleted_features.append(CommitOutcomeItem(data=feature_id, layer_def=feature_to_delete.layer_def, handle=feature_to_delete.handle))
                    except:
                        formatted_exception = logging.traceback.format_exc()
                        logging.error(formatted_exception)
                        transaction_failures.append(CommitOutcomeItem(data=formatted_exception, layer_def=feature_to_delete.layer_def, handle=feature_to_delete.handle))

                yield delete_feature_work()

        cooperator = task.Cooperator()

        work = work_iter()

        yield defer.gatherResults([cooperator.coiterate(work) for _ignored in range(TRANSACTION_COMMIT_PARALLELISM)])

        yield self._expire_all_areas_cache(farm_os_client)

        return TransactionOutcome(
            inserted_features=inserted_features,
            updated_features=updated_features,
            deleted_features=deleted_features,
            transaction_failures=transaction_failures
        )

    @defer.inlineCallbacks
    def _cached_get_all_areas(self, farm_os_client):
        cache_cell = yield self._get_all_areas_cache_cell(id(farm_os_client))

        all_areas = cache_cell.value

        if not all_areas is None:
            return all_areas

        yield cache_cell.lock.acquire()
        try:
            if not cache_cell.value is None:
                return cache_cell.value

            all_areas = yield farm_os_client.area.get_all()

            cache_cell.value = all_areas

            return all_areas
        finally:
            cache_cell.lock.release()

    @defer.inlineCallbacks
    def _expire_all_areas_cache(self, farm_os_client):
        cache_cell = yield self._get_all_areas_cache_cell(id(farm_os_client))

        yield cache_cell.lock.run(setattr, cache_cell, 'value', None)


def main(reactor):
    parser = argparse.ArgumentParser()
    parser.add_argument("--farm-os-url", help="The url for connecting to FarmOS", type=str, default='http://localhost:80')
    parser.add_argument("--proxy-spec", help="The specification for hosting the proxy port", type=str, default='tcp:5707')
    args = parser.parse_args()

    log.startLogging(sys.stdout)

    application = service.Application('FarmOsAreaFeatureProxy', uid=1, gid=1)

    service_collection = service.IServiceCollection(application)

    site = server.Site(WfsResource(FarmOsProxyFeatureServer(args.farm_os_url)))

    svc = strports.service(args.proxy_spec, site)
    svc.setServiceParent(service_collection)

    try:
        svc.startService()
        reactor.run()
    finally:
        svc.stopService()

if __name__ == "__main__":
    main(reactor)

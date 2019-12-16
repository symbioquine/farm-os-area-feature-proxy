#!/bin/env python3

import sys, argparse, logging
from functools import partial, lru_cache, wraps

from twisted.application import service
from twisted.python import log
from twisted.web import server
from twisted.web.resource import Resource
from twisted.internet import reactor, defer
from twisted.web.server import NOT_DONE_YET

from lxml import etree, objectify
from lxml.builder import E, ElementMaker  # lxml only !

WFS_PROTOCOL_VERSION = "1.1.0"
WFS_PROJECTION = "EPSG:4326"
WFS_MIMETYPE = "text/xml"
GML_VERSION = "gml/3.1.1"

NAMESPACES = {
    'gml': "http://www.opengis.net/gml",
    'ms': "http://mapserver.gis.umn.edu/mapserver",
    'ogc': "http://www.opengis.net/ogc",
    'ows': "http://www.opengis.net/ows",
    'wfs': "http://www.opengis.net/wfs",
    'xlink': "http://www.w3.org/1999/xlink",
    'xsd': "http://www.w3.org/2001/XMLSchema",
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
}

from osgeo import ogr, osr

from tx_farm_os_client import TxFarmOsClient


class CallableAccessor(object):
    def __init__(self, c):
        self._c = c

    def __getattr__(self, name):
        return self._c(name)

def DictAccessor(d):
    return CallableAccessor(lambda name: d.get(name))

def PartialAccessor(fn):
    return CallableAccessor(lambda name: partial(fn, name))

def attr(name, value):
    return {name: value}

def ns_attr_partial(namespace):
    return PartialAccessor(lambda attr_name, attr_value: attr("{{{namespace}}}{attr_name}".format(namespace=namespace, attr_name=attr_name), attr_value))

def ns_tag_partial(namespace):
    return CallableAccessor(lambda tag_name: "{{{namespace}}}{tag_name}".format(namespace=namespace, tag_name=tag_name))

ns = DictAccessor(NAMESPACES)
nsE = DictAccessor({k: ElementMaker(namespace=v, nsmap=NAMESPACES) for k, v in NAMESPACES.items()})
nsAttr = DictAccessor({k: ns_attr_partial(v) for k, v in NAMESPACES.items()})
nsTag = DictAccessor({k: ns_tag_partial(v) for k, v in NAMESPACES.items()})

bare = ElementMaker(nsmap=NAMESPACES)
wfs = nsE.wfs
ows = nsE.ows
gml = nsE.gml
ms = nsE.ms
ogc = nsE.ogc

FEATURE_MEMBER_GEO_TYPES = {
    'farm_os_features_point': 'point',
    'farm_os_features_polygon': 'polygon',
    'farm_os_features_line_string': 'linestring'
}

def to_feature_member(type_name, feature):
    geofield = feature.get('geofield', [])

    if len(geofield) != 1:
        return None

    expected_geo_type = FEATURE_MEMBER_GEO_TYPES.get(type_name, None)

    if not expected_geo_type:
        return None

    if geofield[0]['geo_type'] != expected_geo_type:
        return None

    area_name = feature.get('name')
    area_type = feature.get('area_type') or ''
    area_description = feature.get('description') or ''
    area_id = feature.get('tid')

    geom = ogr.CreateGeometryFromWkt(geofield[0].get('geom'))

    srs = osr.SpatialReference()
    srs.SetFromUserInput(WFS_PROJECTION)

    geom.AssignSpatialReference( srs )

    return gml.featureMember(
        ms(type_name,
            ms.geometry(
                etree.XML(geom.ExportToGML(options = ['FORMAT=GML3Deegree', 'SWAP_COORDINATES=NO', 'NAMESPACE_DECL=YES']))
            ),
            ms.area_name(area_name),
            ms.area_type(area_type),
            ms.description(area_description),
            nsAttr.gml.id("{type_name}.{area_id}".format(type_name=type_name, area_id=area_id))
        )
    )

def to_type_filtered_feature_members(type_name, features):
    feature_members = map(partial(to_feature_member, type_name), features)

    return filter(lambda feature: feature is not None, feature_members)

def deferred_rendering_fn(f):
    @wraps(f)
    def wrapper(self, request):
        @defer.inlineCallbacks
        def _inner(request):
            try:
                result = yield defer.maybeDeferred(f, self, request)
                request.write(result)
            except:
                logging.error(logging.traceback.format_exc())
                request.setResponseCode(500)
            request.finish()
        _inner(request)
        return NOT_DONE_YET
    return wrapper

class FarmOsAreaFeatureProxy(Resource):
    isLeaf = True

    def __init__(self, farm_os_url):
        Resource.__init__(self)
        farm_os_client_creation_lock = defer.DeferredLock()

        self._create_farm_os_client = partial(farm_os_client_creation_lock.run,
                                              lru_cache(maxsize=32)(partial(TxFarmOsClient.create, farm_os_url, user_agent="FarmOsAreaFeatureProxy")))

    @deferred_rendering_fn
    @defer.inlineCallbacks
    def render_GET(self, request):
        # Make sure this fn is always a generator
        yield defer.succeed(True)

        args = {k.lower(): v for k, v in request.args.items()}

        request_type = args.get(b'request')[0]

        doc = None
        if request_type == b'GetCapabilities':
            doc = yield self._get_capabilities()
        elif request_type == b'DescribeFeatureType':
            doc = yield self._describe_feature_type(args)
        elif request_type == b'GetFeature':
            doc = yield self._get_feature(request, args)

        if not doc is None:
            return self._etree_response(request, doc)

    @deferred_rendering_fn
    @defer.inlineCallbacks
    def render_POST(self, request):
        transaction = objectify.parse(request.content).getroot()

        if transaction.tag != nsTag.wfs.Transaction:
            raise Exception("Unsupported post request body root: " + transaction.tag)

        farm_os_client = yield self._create_farm_os_client(request.getUser(), request.getPassword())

        inserted_feature_results = []
        total_updated = 0
        total_deleted = 0

        for action in transaction.iterchildren():
            if action.tag == nsTag.wfs.Insert:
                for feature in action.iterchildren():
                    area_name = feature['area_name'].text
                    area_description = getattr(getattr(feature, 'description', None), 'text', '')
                    area_type = getattr(getattr(feature, 'area_type', None), 'text', 'other')

                    geos = list(feature.geometry.iterchildren())

                    if len(geos) != 1:
                        continue

                    geometry = ogr.CreateGeometryFromGML(etree.tostring(geos[0]).decode("utf-8"))

                    record = {
                        "name": area_name,
                        "description": area_description,
                        "area_type": area_type,
                        "geofield": [
                            {
                                "geom": geometry.ExportToWkt()
                            }
                        ]
                    }

                    try:
                        response = yield farm_os_client.area.create(record)

                        feature_type = etree.QName(feature.tag).localname

                        inserted_feature_results.append(
                            wfs.Feature(
                                ogc.FeatureId(fid=feature_type + "." + response.get('id'))
                            )
                        )
                    except:
                        logging.error(logging.traceback.format_exc())
                        # TODO: Add error in results
                        pass

            elif action.tag == nsTag.wfs.Update:

                properties = {}

                for update_property in action[nsTag.wfs.Property]:
                    property_name = update_property.Name.text

                    if property_name == "geometry":
                        geos = list(update_property.Value.iterchildren())

                        if len(geos) != 1:
                            continue

                        geometry = ogr.CreateGeometryFromGML(etree.tostring(geos[0]).decode("utf-8"))

                        properties['geofield'] = [
                            {
                                "geom": geometry.ExportToWkt()
                            }
                        ]

                    if property_name == "area_name":
                        properties['name'] = update_property.Value.text

                    else:
                        properties[property_name] = update_property.Value.text

                for filters in action[nsTag.ogc.Filter]:
                    for update_filter in filters.iterchildren():
                        if update_filter.tag != nsTag.ogc.FeatureId:
                            raise Exception("Only updating by feature id is supported")

                        feature_id = update_filter.get('fid')

                        numeric_feature_id = feature_id.rsplit('.', 1)[1]

                        record = {}

                        record.update(properties)

                        try:
                            yield farm_os_client.area.update(numeric_feature_id, record)
                            total_updated += 1
                        except:
                            logging.error(logging.traceback.format_exc())
                            # TODO: Add error in results
                            pass

            elif action.tag == nsTag.wfs.Delete:
                filters = action[nsTag.ogc.Filter]

                for deletion_filter in filters.iterchildren():
                    if deletion_filter.tag != nsTag.ogc.FeatureId:
                        raise Exception("Only deletion by feature id is supported")

                    feature_id = deletion_filter.get('fid')

                    numeric_feature_id = feature_id.rsplit('.', 1)[1]

                    try:
                        yield farm_os_client.area.delete(numeric_feature_id)
                        total_deleted += 1
                    except:
                        logging.error(logging.traceback.format_exc())
                        # TODO: Add error in results
                        pass

        return self._etree_response(request, wfs.TransactionResponse(
            nsAttr.xsi.schemaLocation("{ns.wfs} http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd".format(ns=ns, WFS_PROTOCOL_VERSION=WFS_PROTOCOL_VERSION)),
            wfs.TransactionSummary(
                wfs.totalInserted(str(len(inserted_feature_results))),
                wfs.totalUpdated(str(total_updated)),
                wfs.totalDeleted(str(total_deleted))
            ),
            wfs.TransactionResult(
                wfs.Status(
                    wfs.SUCCESS()
                )
            ),
            wfs.InsertResults(*inserted_feature_results),
            * [wfs.InsertResult(insert_result) for insert_result in inserted_feature_results],
            version=WFS_PROTOCOL_VERSION
        ))

    def _etree_response(self, request, response_doc):
        request.setHeader('Content-Type', WFS_MIMETYPE)
        request.setResponseCode(code=200)
        etree.cleanup_namespaces(response_doc)
        return etree.tostring(response_doc, pretty_print=True)

    @defer.inlineCallbacks
    def _get_feature(self, request, args):
        type_name = args.get(b'typename')[0].decode('utf-8')

        farm_os_client = yield self._create_farm_os_client(request.getUser(), request.getPassword())

        areas = yield farm_os_client.area.get_all()

        type_filtered_feature_members = to_type_filtered_feature_members(type_name, areas)

        return wfs.FeatureCollection(
            nsAttr.xsi.schemaLocation(("http://mapserver.gis.umn.edu/mapserver "
                                      + "http://localhost:5707?SERVICE=WFS&VERSION={WFS_PROTOCOL_VERSION}&REQUEST=DescribeFeatureType&TYPENAME={type_name}&OUTPUTFORMAT={WFS_MIMETYPE}; "
                                      + "subtype={GML_VERSION} http://www.opengis.net/wfs http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd").format(
                                          WFS_PROTOCOL_VERSION=WFS_PROTOCOL_VERSION,
                                          WFS_MIMETYPE=WFS_MIMETYPE,
                                          GML_VERSION=GML_VERSION,
                                          type_name=type_name)),
            *type_filtered_feature_members
        )

    @defer.inlineCallbacks
    def _describe_feature_type(self, args):
        # Make sure this fn is always a generator
        yield defer.succeed(True)

        type_name = args.get(b'typename')[0].decode('utf-8')

        geometry_type = {
            'farm_os_features_point': 'PointPropertyType',
            'farm_os_features_polygon': 'PolygonPropertyType',
            'farm_os_features_line_string': 'LineStringPropertyType'
        }.get(type_name, 'GeometryPropertyType')

        return bare.schema(
            E("import",
                namespace=ns.gml,
                schemaLocation="http://schemas.opengis.net/{GML_VERSION}/base/gml.xsd".format(GML_VERSION=GML_VERSION)
            ),
            E.element(
                name=type_name,
                type="ms:{}Type".format(type_name),
                substitutionGroup="gml:_Feature"
            ),
            E.complexType(
                E.complexContent(
                    E.extension(
                        E.sequence(
                            E.element(name="geometry", type="gml:" + geometry_type, minOccurs="0", maxOccurs="1"),
                            E.element(name="area_name", type="string"),
                            E.element(name="area_type", type="string"),
                            E.element(name="description", type="string")
                        ),
                        base="gml:AbstractFeatureType"
                    )
                ),
                name="{}Type".format(type_name)
            ),
            xmlns=ns.xsd,
            targetNamespace=ns.ms,
            elementFormDefault="qualified",
            version="0.1"
        )

    @defer.inlineCallbacks
    def _get_capabilities(self):
        return wfs.WFS_Capabilities(
            ows.ServiceIdentification(
                ows.ServiceTypeVersion(WFS_PROTOCOL_VERSION)
            ),
            ows.OperationsMetadata(
                ows.Parameter(
                    ows.Value("WFS"),
                    name="service"
                ),
                ows.Parameter(
                    ows.Value(WFS_PROTOCOL_VERSION),
                    name="AcceptVersions"
                ),
                ows.Parameter(
                    ows.Value(WFS_MIMETYPE),
                    name="AcceptFormats"
                )
            ),
            E.FeatureTypeList(
                E.Operations(E.Query, E.Insert, E.Update, E.Delete),
                *[ E.FeatureType(
                        E.Name("farm_os_features_{}".format(type_name)),
                        E.Title("FarmOS {} features".format(type_name.replace('_', ' '))),
                        E.DefaultSRS(WFS_PROJECTION),
                        E.OutputFormats(
                            E.Format("{WFS_MIMETYPE}; subtype={GML_VERSION}".format(WFS_MIMETYPE=WFS_MIMETYPE, GML_VERSION=GML_VERSION))
                        )
                    ) for type_name in ('point', 'polygon', 'line_string') ]
            ),
            version=WFS_PROTOCOL_VERSION
        )


class FarmOsAreaFeatureProxyService(service.Service):
    def __init__(self, port_num, farm_os_url):
        self._port_num = port_num
        self._farm_os_url = farm_os_url

    def startService(self):
        self._port = reactor.listenTCP(self._port_num, server.Site(FarmOsAreaFeatureProxy(self._farm_os_url)))

    def stopService(self):
        return self._port.stopListening()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--farm-os-url", help="The url for connecting to FarmOS", type=str, default='http://localhost:80')
    args = parser.parse_args()

    log.startLogging(sys.stdout)

    application = service.Application("FarmOsAreaFeatureProxy")
    service = FarmOsAreaFeatureProxyService(5707, args.farm_os_url)
    service.setServiceParent(application)

    service.startService()
    try:
        reactor.run()
    finally:
        service.stopService()

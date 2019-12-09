#!/bin/env python3

from functools import partial

from twisted.application import service
from twisted.web import server
from twisted.web.static import File
from twisted.web.resource import Resource
from twisted.internet import reactor

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

import farmOS, json, re

from itertools import chain

class DictAccessor(object):
    def __init__(self, d):
        self._d = d

    def __getattr__(self, name):
        return self._d.get(name)

class PartialAccessor(object):
    def __init__(self, fn):
        self._fn = fn

    def __getattr__(self, name):
        return partial(self._fn, name)

def attr(name, value):
    return {name: value}

def ns_attr_partial(namespace):
    return PartialAccessor(lambda attr_name, attr_value: attr("{{{namespace}}}{attr_name}".format(namespace=namespace, attr_name=attr_name), attr_value))


ns = DictAccessor(NAMESPACES)
nsE = DictAccessor({k: ElementMaker(namespace=v, nsmap=NAMESPACES) for k, v in NAMESPACES.items()})
nsAttr = DictAccessor({k: ns_attr_partial(v) for k, v in NAMESPACES.items()})

bare = ElementMaker(nsmap=NAMESPACES)
wfs = nsE.wfs
ows = nsE.ows
gml = nsE.gml
ms = nsE.ms


AREA_DESCRIPTION_ID_PATTERN = re.compile('area-details-(\\d+)')

class PointFeatureMemberFactory(object):
    relevant_type_name = "Point"

    def to_feature_member(self, type_name, feature):
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})

        area_name = etree.fromstring(properties.get('name', '')).text
        area_id_match = AREA_DESCRIPTION_ID_PATTERN.match(objectify.fromstring(properties.get('description', '')).get('id'))

        if not area_id_match:
            return None

        area_id = area_id_match.group(1)

        coordinates = geometry.get('coordinates', None)

        return gml.featureMember(
            ms.farm_os_features_point(
                ms.geometry(
                    gml.Point(
                        gml.pos(
                            "{} {}".format(coordinates[0], coordinates[1])
                        ),
                        srsName=WFS_PROJECTION
                    )
                ),
                ms.area_name(area_name),
                nsAttr.gml.id("{type_name}.{area_id}".format(type_name=type_name, area_id=area_id))
            )
        )

class PolygonFeatureMemberFactory(object):
    relevant_type_name = "Polygon"

    def to_feature_member(self, type_name, feature):
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})

        area_name = etree.fromstring(properties.get('name', '')).text
        area_id_match = AREA_DESCRIPTION_ID_PATTERN.match(objectify.fromstring(properties.get('description', '')).get('id'))

        if not area_id_match:
            return None

        area_id = area_id_match.group(1)

        coordinates = geometry.get('coordinates', [])

        pos_list = " ".join(map(str, (chain.from_iterable(chain.from_iterable(coordinates)))))

        return gml.featureMember(
            ms.farm_os_features_polygon(
                ms.geometry(
                    gml.Polygon(
                        gml.exterior(
                            gml.LinearRing(
                                gml.posList(
                                    pos_list,
                                    srsDimension="2"
                                )
                            )
                        ),
                        srsName=WFS_PROJECTION
                    )
                ),
                ms.site_name(area_name),
                nsAttr.gml.id("{type_name}.{area_id}".format(type_name=type_name, area_id=area_id))
            )
        )

class PointStringFeatureMemberFactory(object):
    relevant_type_name = "LineString"

    def to_feature_member(self, type_name, feature):
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})

        area_name = etree.fromstring(properties.get('name', '')).text
        area_id_match = AREA_DESCRIPTION_ID_PATTERN.match(objectify.fromstring(properties.get('description', '')).get('id'))

        if not area_id_match:
            return None

        area_id = area_id_match.group(1)

        coordinates = geometry.get('coordinates', [])

        pos_list = " ".join(map(str, (chain.from_iterable(coordinates))))

        return gml.featureMember(
            ms.farm_os_features_line_string(
                ms.geometry(
                    gml.LineString(
                        gml.posList(
                            pos_list,
                            srsDimension="2"
                        ),
                        srsName=WFS_PROJECTION
                    )
                ),
                ms.site_name(area_name),
                nsAttr.gml.id("{type_name}.{area_id}".format(type_name=type_name, area_id=area_id))
            )
        )

FEATURE_MEMBER_FACTORIES = {
    'farm_os_features_point': PointFeatureMemberFactory(),
    'farm_os_features_polygon': PolygonFeatureMemberFactory(),
    'farm_os_features_line_string': PointStringFeatureMemberFactory()
}

def relevant(factory, feature):
    geometry = feature.get('geometry', {})
    geometry_type = geometry.get('type', None)

    return geometry_type == factory.relevant_type_name

def to_type_filtered_feature_members(type_name, features):
    factory = FEATURE_MEMBER_FACTORIES.get(type_name, None)

    if not factory:
        return ()

    relevant_features = filter(partial(relevant, factory), features)

    feature_members = map(partial(factory.to_feature_member, type_name), relevant_features)

    return filter(None, feature_members)

class FarmOsAreaFeatureProxy(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        args = {k.lower(): v for k, v in request.args.items()}

        print(args)

        request_type = args.get(b'request')[0]

        doc = None
        if request_type == b'GetCapabilities':
            doc = self._get_capabilities()
        elif request_type == b'DescribeFeatureType':
            doc = self._describe_feature_type(args)
        elif request_type == b'GetFeature':
            doc = self._get_feature(request, args)

        if not doc is None:
            request.setHeader('Content-Type', 'text/xml')
            request.setResponseCode(code=200)
            etree.cleanup_namespaces(doc)
            return etree.tostring(doc, pretty_print=True)
        
    def render_POST(self, request):
        args = {k.lower(): v for k, v in request.args.items()}

        print(args)

        print(request.content.read())

    def _get_feature(self, request, args):
        type_name = args.get(b'typename')[0].decode('utf-8')

        farm = farmOS.farmOS('http://172.17.0.2', request.getUser(), request.getPassword())
        farm.authenticate()

        areas = farm.session.http_request("farm/areas/geojson").json()

        type_filtered_feature_members = to_type_filtered_feature_members(type_name, areas.get('features', []))

        return wfs.FeatureCollection(
            nsAttr.xsi.schemaLocation(("http://mapserver.gis.umn.edu/mapserver "
                                      + "http://localhost:5707?SERVICE=WFS&VERSION={WFS_PROTOCOL_VERSION}&REQUEST=DescribeFeatureType&TYPENAME={type_name}&OUTPUTFORMAT={WFS_MIMETYPE}; "
                                      + "subtype={GML_VERSION} http://www.opengis.net/wfs http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd").format(
                                          WFS_PROTOCOL_VERSION=WFS_PROTOCOL_VERSION,
                                          WFS_MIMETYPE=WFS_MIMETYPE,
                                          GML_VERSION=GML_VERSION,
                                          type_name=type_name)),
            gml.boundedBy(
                gml.Envelope(
                    gml.lowerCorner("-122.9273871146143 48.6652415209444"),
                    gml.upperCorner("-122.9273381642997 48.6652226989178"),
                    srsName=WFS_PROJECTION
                )
            ),
            *type_filtered_feature_members
        )

    def _describe_feature_type(self, args):
        type_name = args.get(b'typename')[0].decode('utf-8')

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
                            E.element(name="geometry", type="gml:GeometryPropertyType", minOccurs="0", maxOccurs="1"),
                            E.element(name="area_name", type="string")
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
                E.Operations(E.Query, E.Insert, E.Unsert, E.Delete, E.Lock),
                E.FeatureType(
                    E.Name("farm_os_features_point"),
                    E.Title("FarmOS point features"),
                    E.DefaultSRS(WFS_PROJECTION),
                    E.OutputFormats(
                        E.Format("{WFS_MIMETYPE}; subtype={GML_VERSION}".format(WFS_MIMETYPE=WFS_MIMETYPE, GML_VERSION=GML_VERSION))
                    ),
                    ows.WGS84BoundingBox(
                        ows.LowerCorner("-122.9273871146143 48.6652415209444"),
                        ows.UpperCorner("-122.9273381642997 48.6652226989178"),
                        dimensions="2"
                    )
                ),
                E.FeatureType(
                    E.Name("farm_os_features_polygon"),
                    E.Title("FarmOS polygon features"),
                    E.DefaultSRS(WFS_PROJECTION),
                    E.OutputFormats(
                        E.Format("{WFS_MIMETYPE}; subtype={GML_VERSION}".format(WFS_MIMETYPE=WFS_MIMETYPE, GML_VERSION=GML_VERSION))
                    ),
                    ows.WGS84BoundingBox(
                        ows.LowerCorner("-122.9273871146143 48.6652415209444"),
                        ows.UpperCorner("-122.9273381642997 48.6652226989178"),
                        dimensions="2"
                    )
                ),
                E.FeatureType(
                    E.Name("farm_os_features_line_string"),
                    E.Title("FarmOS line string features"),
                    E.DefaultSRS(WFS_PROJECTION),
                    E.OutputFormats(
                        E.Format("{WFS_MIMETYPE}; subtype={GML_VERSION}".format(WFS_MIMETYPE=WFS_MIMETYPE, GML_VERSION=GML_VERSION))
                    ),
                    ows.WGS84BoundingBox(
                        ows.LowerCorner("-122.9273871146143 48.6652415209444"),
                        ows.UpperCorner("-122.9273381642997 48.6652226989178"),
                        dimensions="2"
                    )
                )
            ),
            version=WFS_PROTOCOL_VERSION
        )


class FarmOsAreaFeatureProxyService(service.Service):

    def __init__(self, portNum):
        self.portNum = portNum

    def startService(self):
        self._port = reactor.listenTCP(self.portNum, server.Site(FarmOsAreaFeatureProxy()))

    def stopService(self):
        return self._port.stopListening()


application = service.Application('FarmOsAreaFeatureProxy')
service = FarmOsAreaFeatureProxyService(5707)
service.setServiceParent(application)

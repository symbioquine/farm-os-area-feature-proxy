#!/bin/env python3

from functools import partial

from twisted.application import service
from twisted.web import server
from twisted.web.static import File
from twisted.web.resource import Resource
from twisted.internet import reactor

from lxml import etree
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

import farmOS

farm = farmOS.farmOS('http://172.17.0.2', 'FarmOS.restws.zero', 'zsARb1hZjFwK0jMIh3Td')
success = farm.authenticate()

info = farm.info()

print(info)

areas = farm.session.http_request("farm/areas/geojson")

print(areas.json())

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


class FarmOsAreaFeatureProxy(Resource):
    isLeaf = True

    def __init__(self):
        Resource.__init__(self)

    def render_GET(self, request):
        args = {k.lower(): v for k, v in request.args.items()}

        print(args)

        request_type = args.get(b'request')

        doc = None
        if request_type == [b'GetCapabilities']:
            doc = self._get_capabilities()
        elif request_type == [b'DescribeFeatureType']:
            doc = self._describe_feature_type(request)
        elif request_type == [b'GetFeature']:
            doc = self._get_feature(request)

        if not doc is None:
            request.setHeader('Content-Type', 'text/xml')
            request.setResponseCode(code=200)
            etree.cleanup_namespaces(doc)
            return etree.tostring(doc, pretty_print=True)
        
    def render_POST(self, request):
        args = {k.lower(): v for k, v in request.args.items()}

        print(args)

        print(request.content.read())

    def _get_feature(self, request):
        return wfs.FeatureCollection(
            nsAttr.xsi.schemaLocation(("http://mapserver.gis.umn.edu/mapserver "
                                      + "http://localhost:5707?SERVICE=WFS&VERSION={WFS_PROTOCOL_VERSION}&REQUEST=DescribeFeatureType&TYPENAME=antarctic_ice_shelves_fill&OUTPUTFORMAT={WFS_MIMETYPE}; "
                                      + "subtype={GML_VERSION} http://www.opengis.net/wfs http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd").format(
                                          WFS_PROTOCOL_VERSION=WFS_PROTOCOL_VERSION,
                                          WFS_MIMETYPE=WFS_MIMETYPE,
                                          GML_VERSION=GML_VERSION)),
            gml.boundedBy(
                gml.Envelope(
                    gml.lowerCorner("-122.9273871146143 48.6652415209444"),
                    gml.upperCorner("-122.9273381642997 48.6652226989178"),
                    srsName=WFS_PROJECTION
                )
            ),
            #gml.featureMember(
            #    ms.antarctic_ice_shelves_fill(
            #        ms.geometry(
            #            gml.Point(
            #                gml.pos(
            #                    "-122.9273381642997 48.6652266847594"
            #                ),
            #                srsName=WFS_PROJECTION
            #            )
            #        ),
            #        ms.site_name("Coolidge6"),
            #        nsAttr.gml.id("antarctic_ice_shelves_fill.6")
            #    )
            #),
            #gml.featureMember(
            #    ms.antarctic_ice_shelves_fill(
            #        ms.geometry(
            #            gml.Point(
            #                gml.pos(
            #                    "-122.9273381642997 48.6652266847594"
            #                ),
            #                srsName=WFS_PROJECTION
            #            )
            #        ),
            #        ms.site_name("Coolidge7"),
            #        nsAttr.gml.id("antarctic_ice_shelves_fill.7")
            #    )
            #),
            gml.featureMember(
                ms.antarctic_ice_shelves_fill(
                    ms.geometry(
                        gml.Polygon(
                            gml.exterior(
                                gml.LinearRing(
                                    gml.posList(
                                         "-122.9273381642997 48.6652266847594 "
                                        +"-122.9273411817849 48.66524152094448 "
                                        +"-122.9273871146143 48.66523753510415 "
                                        +"-122.9273844324052 48.66522269891789 "
                                        +"-122.9273381642997 48.6652266847594",
                                        srsDimension="2"
                                    )
                                )
                            ),
                            srsName=WFS_PROJECTION
                        )
                    ),
                    ms.site_name("Coolidge8"),
                    nsAttr.gml.id("antarctic_ice_shelves_fill.8")
                )
            )
        )

    def _describe_feature_type(self, request):
        return bare.schema(
            E("import",
                namespace=ns.gml,
                schemaLocation="http://schemas.opengis.net/{GML_VERSION}/base/gml.xsd".format(GML_VERSION=GML_VERSION)
            ),
            E.element(
                name="antarctic_ice_shelves_fill",
                type="ms:antarctic_ice_shelves_fillType",
                substitutionGroup="gml:_Feature"
            ),
            E.complexType(
                E.complexContent(
                    E.extension(
                        E.sequence(
                            E.element(name="geometry", type="gml:GeometryPropertyType", minOccurs="0", maxOccurs="1"),
                            E.element(name="site_name", type="string")
                        ),
                        base="gml:AbstractFeatureType"
                    )
                ),
                name="antarctic_ice_shelves_fillType"
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
                    E.Name("antarctic_ice_shelves_fill"),
                    E.Title("Antarctic ice shelves"),
                    ows.Keywords(
                        ows.Keyword("Antarctica"),
                        ows.Keyword("Coastlines")
                    ),
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

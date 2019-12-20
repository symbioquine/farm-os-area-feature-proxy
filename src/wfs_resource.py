#!/bin/env python3

import logging

from functools import partial
from itertools import chain, groupby
from operator import attrgetter

from zope.interface import Attribute, Interface, implementer

from twisted.application import service, strports
from twisted.internet import defer
from twisted.python import log
from twisted.web import server
from twisted.web.resource import Resource

from semantic_version import Version

from lxml import etree, objectify
from lxml.builder import E, ElementMaker  # lxml only !

from osgeo import ogr, osr

from deferred_rendering_fn import deferred_rendering_fn

WFS_MIMETYPE = "text/xml"

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
xsd = nsE.xsd


class FeatureField(object):
    """
    Data object describing a field of a feature layer.
    """

    def __init__(self, name, field_type, required=False):
        self.name = name
        """Name of this feature field. (required)"""

        self.field_type = field_type
        """Type of this feature field. (required)"""

        self.required = required
        """Whether this field is required."""


class Feature(object):
    """
    Data object holding the data of a single feature.
    """

    def __init__(self, feature_id, geometry, field_data=None):
        self.feature_id = feature_id
        """Identifier of this feature. (required)"""

        self.geometry = geometry
        """Geometry of this feature. Must be an instance of C{osgeo.ogr.Geometry} with a spatial reference assigned and matching the default spatial reference of its layer. (required)"""

        if not field_data:
            field_data = {}

        self.field_data = dict(**field_data)
        """Field data for this feature. Must be a dictionary containing at least the required fields for a feature of its layer."""


class LayerDefinition(object):
    """
    Data object with the definition of a feature layer.
    """

    def __init__(self, name, default_srs, geometry_type='GeometryPropertyType', title=None, abstract=None, operations=('Query',), fields=(), ext=None):
        self.name = name
        """Name of this feature layer. (required)"""

        self.default_srs = default_srs
        """Default spatial reference system of this feature layer. (required)"""

        self.geometry_type = geometry_type
        """Geometry type of this feature layer."""

        self.title = title
        """Title of this feature layer."""

        self.abstract = abstract
        """Abstract of this feature layer."""

        self.operations = set(operations)
        """Operations supported on this feature layer. Iterable of strings from the set {'Query', 'Insert', 'Update', 'Delete'} Default: {'Query'}"""

        self.fields = tuple(fields)
        """Fields of features in this feature layer. Iterable of L{FeatureField}"""

        if not ext:
            ext = {}

        self.ext = DictAccessor(dict(**ext))
        """Map of extended properties for this layer accessible via layer_def.ext.my_prop"""


class UncommittedFeature(object):
    """
    Data object holding the data of a single uncommitted feature.
    """

    def __init__(self, layer_def, geometry, field_data=None, handle=None):
        self.layer_def = layer_def
        """Layer definition for the layer in which the feature should be inserted. (required)"""

        self.geometry = geometry
        """Geometry of this feature. Must be an instance of C{osgeo.ogr.Geometry} with a spatial reference assigned and matching the default spatial reference of its layer. (required)"""

        if not field_data:
            field_data = {}

        self.field_data = dict(**field_data)
        """Field data for this feature. Must be a dictionary containing at least the required fields for a feature of its layer."""

        self.handle = handle
        """Identifier for this uncommitted feature. (required)"""


class UncommittedFeatureUpdate(object):
    """
    Data object holding the data of a single uncommitted feature.
    """

    def __init__(self, layer_def, feature_id, geometry=None, field_data=None, handle=None):
        self.layer_def = layer_def
        """Layer definition for the layer in which the feature should be updated. (required)"""

        self.feature_id = feature_id
        """Identifier of the feature to update. (required)"""

        self.geometry = geometry
        """Updated geometry for this feature. Must be an instance of C{osgeo.ogr.Geometry} with a spatial reference assigned and matching the default spatial reference of its layer. None if the geometry of the feature is not being updated."""

        if not field_data:
            field_data = {}

        self.field_data = dict(**field_data)
        """Updated field data for this feature. Only contains the fields to be updated."""

        self.handle = handle
        """Identifier for this uncommitted feature update."""


class UncommittedFeatureDelete(object):
    """
    Data object describing a feature to delete.
    """

    def __init__(self, layer_def, feature_id, handle=None):
        self.layer_def = layer_def
        """Layer definition for the layer from which the feature should be deleted. (required)"""

        self.feature_id = feature_id
        """Identifier of the feature to upate. (required)"""

        self.handle = handle
        """Identifier for this uncommitted feature update."""

class Transaction(object):
    """
    Data object with the changes to commit.
    """

    def __init__(self, features_to_insert=(), features_to_update=(), features_to_delete=(), read_transaction_failures=()):
        self.features_to_insert = tuple(features_to_insert)
        """Iterable of L{UncommittedFeature}"""

        self.features_to_update = tuple(features_to_update)
        """Iterable of L{UncommittedFeatureUpdate}"""

        self.features_to_delete = tuple(features_to_delete)
        """Iterable of L{UncommittedFeatureDelete}"""

        self.read_transaction_failures = tuple(read_transaction_failures)
        """Iterable of L{CommitOutcomeItem} indicating this transaction object is incomplete due to partial failures
        while reading the request. Truly transactional C{IFeatureServer} implementations should probably return right
        away when this is not empty."""


class CommitOutcomeItem(object):
    """
    Data object wrapping some aspect of a transaction outcome - context specific - and possibly
    associating the handle for that part of the transaction.
    """

    def __init__(self, data, layer_def=None, handle=None):
        self.data = data
        """Data of this outcome item. (required)"""

        self.layer_def = layer_def
        """Layer definition for the layer that this outcome item is related to if determinable."""

        self.handle = handle
        """Identifier for this uncommitted feature update."""


class TransactionOutcome(object):
    """
    Data object with the outcome of a transaction to a feature layer. At least one of the attributes must contain
    one or more instances of L{CommitOutcomeItem}. A truly transactional C{IFeatureServer} implementation would
    either populate just the transaction_failures attribute or just the others.
    """

    def __init__(self, inserted_features=(), updated_features=(), deleted_features=(), transaction_failures=()):
        self.inserted_features = tuple(inserted_features)
        """Iterable of L{CommitOutcomeItem} wrapping the feature ids of the newly inserted features."""

        self.updated_features = tuple(updated_features)
        """Iterable of L{CommitOutcomeItem} wrapping the feature ids of the updated features."""

        self.deleted_features = tuple(deleted_features)
        """Iterable of L{CommitOutcomeItem} wrapping the feature ids of the deleted features."""

        self.transaction_failures = tuple(transaction_failures)
        """Iterable of L{CommitOutcomeItem} wrapping the failure messages for the full or partial transaction failure."""


class IFeatureServer(Interface):
    """
    Surfaces features in a protocol agnostic manner.
    """
    name = Attribute("Name of this feature server.")
    title = Attribute("Title of this feature server.")
    abstract = Attribute("Abstract of this feature server.")
    keywords = Attribute("Keywords for this feature server.")

    def layer_definitions(self, request):
        """
        Returns an iterable of L{LayerDefinition} for the supported layers of
        this feature server. Can optionally return a deferred, but will be called
        frequently so should not be expensive.

        @param request: The request for which the layer definitions are being requested.
           Implementations are expected to avoid parsing anything WFS-related out of the
           request, but may honor headers, authentication state, etc.
        @type request: C{twisted.web.http.Request}
        """

    def get_all_features(self, layer_def, request):
        """
        Get all features for a given layer. Can optionally return a deferred.

        @param layer_def: The layer to get features for.
        @type layer_def: L{LayerDefinition}

        @param request: The request for which the features are being retrieved.
           Implementations are expected to avoid parsing anything WFS-related out of the
           request, but may honor headers, authentication state, etc.
        @type request: C{twisted.web.http.Request}

        @return: An iterable of L{Feature}
        """

    def commit_transaction(self, transaction, request):
        """
        Commit a transaction to a layer. Can optionally return a deferred.

        @param transaction: The transaction to commit.
        @type transaction: L{Transaction}

        @param request: The request for which the transaction is being committed.
           Implementations are expected to avoid parsing anything WFS-related out of the
           request, but may honor headers, authentication state, etc.
        @type request: C{twisted.web.http.Request}

        @return: L{TransactionOutcome}
        """


class WfsResource(object):
    isLeaf = False

    def __init__(self, feature_server):
        self._feature_server = feature_server
        self._version_specific_resources = (
            WfsOnePointZeroResource(feature_server),
        )
        self._by_version = {r.version : r for r in self._version_specific_resources}
        self._min_version = min(self._by_version.keys())
        self._max_version = max(self._by_version.keys())

    def putChild(self, path, child):
        raise Exception("WfsResource does not support arbitrary child resources.")

    def getChildWithDefault(self, name, request):
        args = {k.lower(): v for k, v in request.args.items()}

        requested_service = _first(args.get(b'service', ()), None)

        if requested_service != b'WFS':
            raise InvalidWfsRequest("All requests must include a 'service=WFS' argument. Got: {!r}".format(requested_service))

        requested_version = _first(args.get(b'version', ()), None)

        if requested_version:
            requested_version = Version(requested_version.decode('utf-8'))

        if not requested_version:
            response_version = self._max_version
        elif requested_version in self._by_version:
            response_version = requested_version
        else:
            acceptable_versions = list(filter(lambda version: version <= requested_version, self._by_version.keys()))

            response_version = _first(sorted(acceptable_versions, reversed=True), self._min_version)

        return self._by_version[response_version]


class WfsOnePointZeroResource(Resource):
    version = Version('1.0.0')
    gml_version = Version('2.1.2')
    isLeaf = True

    class GetCapabilitiesCapabilityHandler(object):
        capability = b'GetCapabilities'
        methods = {'Get'}
        extra_description_elems = ()

        @defer.inlineCallbacks
        def handle(self, resource, request, args):
            # Make sure this fn is always a generator
            yield defer.succeed(True)

            feature_server = resource._feature_server

            layer_definitions = yield defer.maybeDeferred(feature_server.layer_definitions, request)

            location = request.uri.decode('utf-8')

            def request_type(capability_handler):
                return wfs(capability_handler.capability.decode('utf-8'),
                   *[ wfs.DCPType(wfs.HTTP(wfs(method, onlineResource=location))) for method in capability_handler.methods ],
                   *capability_handler.extra_description_elems
                )

            def attr_elem(obj, attr_name, required=False):
                attr = getattr(obj, attr_name, None)

                if attr is None and required:
                    raise AttributeError("{!r} is expected to have the required attribute {!r}".format(obj, attr_name))

                if attr is None:
                    return ()

                return [wfs(attr_name.capitalize(), attr)]

            return wfs.WFS_Capabilities(
                wfs.Service(
                 * attr_elem(feature_server, 'name', required=True),
                 * attr_elem(feature_server, 'title'),
                 * attr_elem(feature_server, 'abstract'),
                  wfs.OnlineResource(location)
                ),
                wfs.Capability(
                    wfs.Request(
                        *list(map(request_type, resource._capability_handlers.values()))
                    )
                ),
                wfs.FeatureTypeList(
                    *[ wfs.FeatureType(
                            * attr_elem(layer_def, 'name', required=True),
                            * attr_elem(layer_def, 'title'),
                            * attr_elem(layer_def, 'abstract'),
                            wfs.SRS(layer_def.default_srs),
                            wfs.Operations(*[ wfs(operation) for operation in (set(layer_def.operations) & {'Query', 'Insert', 'Update', 'Delete'}) ]),
                            # TODO: consider providing a mechanism to populate 'LatLongBoundingBox' & 'MetadataURL' elements
                        ) for layer_def in layer_definitions ]
                ),
                version=str(resource.version)
            )

    class DescribeFeatureTypeCapabilityHandler(object):
        capability = b'DescribeFeatureType'
        methods = {'Get'}
        extra_description_elems = (wfs.SchemaDescriptionLanguage(wfs.XMLSCHEMA),)

        @defer.inlineCallbacks
        def handle(self, resource, request, args):
            # Make sure this fn is always a generator
            yield defer.succeed(True)

            requested_type_name = _first(args.get(b'typename', ()), b'').decode('utf-8')

            layer_definitions = yield defer.maybeDeferred(resource._feature_server.layer_definitions, request)

            if requested_type_name:
                layer_definitions = list(filter(lambda layer_def: layer_def.name == requested_type_name, layer_definitions))

            return bare.schema(
                xsd("import",
                    namespace=ns.gml,
                    schemaLocation="http://schemas.opengis.net/gml/{GML_VERSION}/feature.xsd".format(GML_VERSION=str(resource.gml_version))
                ),
                *chain.from_iterable([
                    [xsd.element(
                        name=layer_def.name,
                        type="ms:{}Type".format(layer_def.name),
                        substitutionGroup="gml:_Feature"
                    ),
                    xsd.complexType(
                        xsd.complexContent(
                            xsd.extension(
                                xsd.sequence(
                                    xsd.element(name="geometry", type="gml:" + layer_def.geometry_type),
                                    *[ xsd.element(name=field.name, type=field.field_type) for field in layer_def.fields ]
                                ),
                                base="gml:AbstractFeatureType"
                            )
                        ),
                        name="{}Type".format(layer_def.name)
                    )] for layer_def in layer_definitions
                ]),
                xmlns=ns.xsd,
                targetNamespace=ns.ms,
                elementFormDefault="qualified",
                version="0.1"
            )

    class GetFeatureCapabilityHandler(object):
        capability = b'GetFeature'
        methods = {'Get'}
        extra_description_elems = (wfs.ResultFormat(wfs.GML2),)

        @defer.inlineCallbacks
        def handle(self, resource, request, args):
            # Make sure this fn is always a generator
            yield defer.succeed(True)

            requested_type_name = _first(args.get(b'typename', ()), b'').decode('utf-8')

            layer_definitions = yield defer.maybeDeferred(resource._feature_server.layer_definitions, request)

            layer_def = next(filter(lambda layer_def: layer_def.name == requested_type_name, layer_definitions), None)

            if not layer_def:
                raise InvalidWfsRequest("Requested features of an unknown TYPENAME: {!r}".format(requested_type_name))

            features = yield defer.maybeDeferred(resource._feature_server.get_all_features, layer_def, request)

            def to_feature_member(feature):
                return gml.featureMember(
                    ms(layer_def.name,
                        ms.geometry(
                            etree.XML(feature.geometry.ExportToGML(options=['FORMAT=GML2', 'SWAP_COORDINATES=NO', 'NAMESPACE_DECL=YES']))
                        ),
                        *[ ms(field.name, feature.field_data[field.name]) for field in layer_def.fields if field.name in feature.field_data ],
                        fid=feature.feature_id
                    )
                )

            return wfs.FeatureCollection(
                nsAttr.xsi.schemaLocation(("http://mapserver.gis.umn.edu/mapserver "
                                          +"http://localhost:5707?SERVICE=WFS&VERSION={WFS_PROTOCOL_VERSION}&REQUEST=DescribeFeatureType&TYPENAME={type_name}&OUTPUTFORMAT={WFS_MIMETYPE}; "
                                          +"subtype={GML_VERSION} http://www.opengis.net/wfs http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd").format(
                                              WFS_PROTOCOL_VERSION=resource.version,
                                              WFS_MIMETYPE=WFS_MIMETYPE,
                                              GML_VERSION=str(resource.gml_version),
                                              type_name=layer_def.name)),
                *map(to_feature_member, features)
            )

    class TransactionCapabilityHandler(object):
        capability = b'Transaction'
        methods = {'Post'}
        extra_description_elems = ()

        @defer.inlineCallbacks
        def handle(self, resource, request, args):
            # Make sure this fn is always a generator
            yield defer.succeed(True)

            layer_definitions = yield defer.maybeDeferred(resource._feature_server.layer_definitions, request)

            wfs_transaction = objectify.parse(request.content).getroot()

            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(etree.tostring(wfs_transaction, pretty_print=True).decode('utf-8'))

            if wfs_transaction.tag != nsTag.wfs.Transaction:
                raise Exception("Unsupported post request body root: " + wfs_transaction.tag)

            features_to_insert = []
            features_to_update = []
            features_to_delete = []
            wfs_read_transaction_failures = []

            def read_insert_feature(handle, feature):
                type_name = etree.QName(feature.tag).localname

                layer_def = next(filter(lambda layer_def: layer_def.name == type_name, layer_definitions), None)

                if not layer_def:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received invalid feature to insert for unknown TYPENAME: {!r}".format(type_name)))
                    return

                if not 'Insert' in layer_def.operations:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received unsupported operation insert for TYPENAME: {!r}".format(type_name)))
                    return

                geos = list(feature.geometry.iterchildren())

                if len(geos) != 1:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(layer_def=layer_def, handle=handle, data="Received invalid feature to insert with {} geometries".format(len(geos))))
                    return

                # TODO: Catch exceptions from invalid geometry
                geometry = ogr.CreateGeometryFromGML(etree.tostring(geos[0]).decode("utf-8"))

                # TODO: Validate/normalize srs

                missing_required_field_names = []

                def extract_field_items():
                    for field in layer_def.fields:
                        v = getattr(getattr(feature, field.name, None), 'text', None)

                        if v is None and field.required:
                            missing_required_field_names.append(field.name)

                        if v is None:
                            continue

                        yield (field.name, v)

                field_data = {field_name: field_value for (field_name, field_value) in extract_field_items()}

                if missing_required_field_names:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(layer_def=layer_def, handle=handle, data="Received invalid feature to insert with missing required fields: {}".format(missing_required_field_names)))
                    return

                features_to_insert.append(UncommittedFeature(layer_def=layer_def, geometry=geometry, field_data=field_data, handle=handle))

            def read_update(handle, action):
                type_name = etree.QName(action.get('typeName', '')).localname

                layer_def = next(filter(lambda layer_def: layer_def.name == type_name, layer_definitions), None)

                if not layer_def:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received update for unknown TYPENAME: {!r}".format(type_name)))
                    return

                if not 'Update' in layer_def.operations:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received unsupported operation update for TYPENAME: {!r}".format(type_name)))
                    return

                geometry = None
                field_data = {}

                fields_by_name = {field.name: field for field in layer_def.fields}

                for update_property in action[nsTag.wfs.Property]:
                    property_name = update_property.Name.text

                    if property_name == "geometry":
                        geos = list(update_property.Value.iterchildren())

                        if len(geos) != 1:
                            wfs_read_transaction_failures.append(CommitOutcomeItem(layer_def=layer_def, handle=handle, data="Received invalid feature update with {} geometries".format(len(geos))))
                            continue

                        # TODO: Catch exceptions from invalid geometry
                        geometry = ogr.CreateGeometryFromGML(etree.tostring(geos[0]).decode("utf-8"))

                        # TODO: Validate/normalize srs
                    else:
                        field = fields_by_name.get(property_name, None)

                        if field is None:
                            continue

                        field_data[property_name] = update_property.Value.text

                for filters in action[nsTag.ogc.Filter]:
                    for update_filter in filters.iterchildren():

                            if update_filter.tag != nsTag.ogc.FeatureId:
                                wfs_read_transaction_failures.append(CommitOutcomeItem(layer_def=layer_def, handle=handle, data="Only updating features by feature id is supported"))
                                return

                            feature_id = update_filter.get('fid')

                            features_to_update.append(UncommittedFeatureUpdate(layer_def=layer_def, feature_id=feature_id, geometry=geometry, field_data=field_data, handle=handle))

            def read_delete(handle, action):
                type_name = etree.QName(action.get('typeName', '')).localname

                layer_def = next(filter(lambda layer_def: layer_def.name == type_name, layer_definitions), None)

                if not layer_def:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received delete for unknown TYPENAME: {!r}".format(type_name)))
                    return

                if not 'Delete' in layer_def.operations:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received unsupported operation delete for TYPENAME: {!r}".format(type_name)))
                    return

                filters = action[nsTag.ogc.Filter]

                for deletion_filter in filters.iterchildren():

                    if deletion_filter.tag != nsTag.ogc.FeatureId:
                        wfs_read_transaction_failures.append(CommitOutcomeItem(layer_def=layer_def, handle=handle, data="Only deleting features by feature id is supported"))
                        return

                    feature_id = deletion_filter.get('fid')

                    features_to_delete.append(UncommittedFeatureDelete(layer_def=layer_def, feature_id=feature_id))

            for action in wfs_transaction.iterchildren():

                handle = action.get('handle', None)

                if action.tag == nsTag.wfs.Insert:
                    for feature in action.iterchildren():
                        read_insert_feature(handle, feature)

                elif action.tag == nsTag.wfs.Update:
                    read_update(handle, action)

                elif action.tag == nsTag.wfs.Delete:
                    read_delete(handle, action)

                else:
                    wfs_read_transaction_failures.append(CommitOutcomeItem(handle=handle, data="Received unknown operation type: ".format(action.tag)))


            transaction = Transaction(features_to_insert=features_to_insert,
                                      features_to_update=features_to_update,
                                      features_to_delete=features_to_delete,
                                      read_transaction_failures=wfs_read_transaction_failures)

            transaction_outcome = yield defer.maybeDeferred(resource._feature_server.commit_transaction, transaction, request)

            insert_results_by_handle = _group_by_handle(transaction_outcome.inserted_features)
            all_transaction_failures = list(chain(wfs_read_transaction_failures, transaction_outcome.transaction_failures))

            any_successes = (len(transaction_outcome.inserted_features) + len(transaction_outcome.updated_features) + len(transaction_outcome.deleted_features)) > 0

            if any_successes and not all_transaction_failures:
                status = wfs.SUCCESS()
            elif any_successes:
                status = wfs.PARTIAL()
            else:
                status = wfs.FAILED()

            def optional_handle_attribute_for(insert_results):
                handle = getattr(_first(insert_results, None), 'handle', '')

                if not handle:
                    return []

                return [attr('handle', handle)]

            return wfs.TransactionResponse(
                nsAttr.xsi.schemaLocation("{ns.wfs} http://schemas.opengis.net/wfs/{WFS_PROTOCOL_VERSION}/wfs.xsd".format(ns=ns, WFS_PROTOCOL_VERSION=str(resource.version))),
                wfs.TransactionResult(
                    wfs.Status(status),
                    *[ wfs.Message(transaction_failure.data) for transaction_failure in all_transaction_failures ]
                ),
                *[ wfs.InsertResult(
                       *[ ogc.FeatureId(fid=insert_result.data) for insert_result in insert_results ],
                       *optional_handle_attribute_for(insert_results)
                   ) for handle, insert_results in insert_results_by_handle
                ],
                version=str(resource.version)
            )

    def __init__(self, feature_server):
        self._feature_server = feature_server
        self._capability_handlers = { capability_handler.capability : capability_handler for capability_handler in (
            self.GetCapabilitiesCapabilityHandler(),
            self.DescribeFeatureTypeCapabilityHandler(),
            self.GetFeatureCapabilityHandler(),
            self.TransactionCapabilityHandler(),
        ) }

    @deferred_rendering_fn
    @defer.inlineCallbacks
    def render(self, request):
        # Make sure this fn is always a generator
        yield defer.succeed(True)

        args = {k.lower(): v for k, v in request.args.items()}

        # Assume POST requests should go through the Transaction capability handler
        # for now since that means we only need to read the request.content file-like
        # object once there. This might need to get more complex in the future if
        # we want to support POST for other capabilities in this WFS version.
        if request.method == b'POST':
            capability_handler = self._capability_handlers.get(b'Transaction')
        else:
            request_type = _first(args.get(b'request', ()), None)

            capability_handler = self._capability_handlers.get(request_type, None)

            if not capability_handler:
                raise InvalidWfsRequest("Unsupported capability: {!r}".format(request_type))

            if not request.method.decode('utf-8').lower().capitalize() in capability_handler.methods:
                raise InvalidWfsRequest("Capability: {!r} not supported via HTTP method: {!r}".format(request_type, request.method))

        response_doc = yield capability_handler.handle(self, request, args)

        request.setHeader('Content-Type', WFS_MIMETYPE)
        request.setResponseCode(code=200)
        etree.cleanup_namespaces(response_doc)
        return etree.tostring(response_doc, pretty_print=True)


def _first(iterable, default):
    return next(iter(iterable), default)

def _group_by_handle(items):
    def get_handle(item):
        return item.handle or ''
    for k, g in groupby(sorted(items, key=get_handle)):
        yield k, list(g)

class InvalidWfsRequest(Exception): pass

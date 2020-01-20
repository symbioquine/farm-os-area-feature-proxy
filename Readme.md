# farm-os-area-feature-proxy

farm-os-area-feature-proxy is a stand-alone proxy which makes a standard FarmOS installation accessible as a [Web Feature Service (WFS)](https://www.opengeospatial.org/standards/wfs) which can be used in programs such as [Quantum GIS](https://qgis.org).

In practical terms, farm-os-area-feature-proxy provides bi-directional data flow between areas configured in FarmOS and vector feature layers in QGIS. This allows farm areas to be viewed/edited in the context of additional data with more powerful mapping tools than FarmOS currently provides.

Here are some examples of things which that might help with;

* Easily use multiple "base maps" such as those provided by county GIS departments rather than just Google/OSM
* Copy property boundaries or other features from existing GIS resources into the map and have them automatically converted to the correct CRS
* Georeference raster images of a property from a drone and use that as a background layer for mapping complex gardens, buildings, hoop houses, etc
* Use existing advanced digitalizing tools and plugins to draw precise dimensions, geometric patterns, angles, etc
* (hypothetically) Import topo data then calculate average angle of incidence, hours of daylight, etc for each field/garden based on the surrounding hills and append this information to the FarmOS area description

**Demo of FarmOS and Quantum GIS connected via the proxy;**

![Peek 2019-12-18 09-11](https://user-images.githubusercontent.com/30754460/71107878-d0bb1300-2176-11ea-9a86-352176e3f6bf.gif)

## Limitations

* Only supports WFS 1.0.0 currently
* Only supports features with single geometries
* Only supports the area name, type, and description fields
* Only supports [EPSG:4326](https://epsg.io/4326) spatial reference system
* Doesn't return extents of feature layers (Mapping tools may not automatically zoom to show all features when first adding the feature layers)

## Getting Started

Add the `area-feature-proxy` service to your [FarmOS docker-compose.yml](https://farmos.org/hosting/docker/) file;

```yaml
  area-feature-proxy:
    depends_on:
      - www
    image: symbioquine/farm-os-area-feature-proxy:0.1.1
    command: --farm-os-url=http://www:80
    ports:
      - '5707:5707'
```

The WFS service will now be running at http://localhost:5707 next time you run `docker-compose up`.

### Use in QGIS

Configure a layer data source with the following parameters;

```
Name: FarmOSExample
URL: http://localhost:5707
Basic Authentication
```

The user name and password should be those of a user on your FarmOS site who is authorized to make restws requests. Useful background can be found at https://farmos.org/development/api/#authentication

## Future Work

* Improve extent handling
* Support GeometryCollection features
* See whether it is possible to model area_type field on features as an enum that QGIS would honor
* See how OAuth2 authentication with FarmOS (ref: [FarmOS#203](https://github.com/farmOS/farmOS/issues/203)) could work (QGIS has a [plugin to support OAuth2](http://docs.opengeospatial.org/per/17-021.pdf), but more investigation is needed to see how transitive authentication could/should work with FarmOS)
* break tx_drupal_rest_ws_client and tx_farm_os_client into separate repositories

## FAQ

### Doesn't FarmOS already support mapping directly?

Yes, but it's convenient to be able to use the FarmOS data directly in fully-fledged GIS tools without importing/exporting the data or allowing direct DB access.

### Why write a stand-alone proxy? Wouldn't it be better to build the WFS server functionality directly into FarmOS?

Possibly, but it would be more costly to get this proof-of-concept into a state where it could be a reasonable pull-request. Also, there are advantages in not bloating FarmOS with functionality that only some users will need.


## Development Testing

You can also run farm-os-area-feature-proxy directly from a checked out copy of this repository;

```bash
docker run --name=farm-os-area-feature-proxy --rm -p 5707:5707 -it $(docker build -q src/) --farm-os-url=http://172.17.0.2:123:80
```

Or when running against the [FarmOS development docker-compose](https://farmos.org/development/docker/) environment;

```bash
docker run --name=farm-os-area-feature-proxy --rm -p 5707:5707 --network=farm-os-development_default -it $(docker build -q src/) --farm-os-url=http://www:80
```

Now the proxy will be running at http://localhost:5707

## Https

Since farm-os-area-feature-proxy handles your FarmOS credentials you should consider your threat-model and probably host a secure endpoint.

*Note: Many use-cases would be better served by an NGINX reverse-proxy which would provide much more control over protocols, ciphers, DH parameters, etc.*

Create dev certificates using [mkcert](https://github.com/FiloSottile/mkcert); *(Obviously, production usage would involve obtaining real certificates - left as an exercise to the reader.)*

```bash
mkdir devcerts && mkcert -key-file devcerts/key.pem -cert-file devcerts/cert.pem farmos.local *.farmos.local localhost 127.0.0.1 ::1
```

Run;

```yaml
  area-feature-proxy:
    depends_on:
      - www
    image: farm-os-area-feature-proxy:0.1.1
    command: --farm-os-url=http://www:80 --proxy-spec="ssl:5707:privateKey=/mnt/certs/key.pem:certKey=/mnt/certs/cert.pem"
    volumes:
      - './devcerts:/mnt/certs'
    ports:
      - '5707:5707'
```

or;

```bash
docker run --name=farm-os-area-feature-proxy --rm -p 5707:5707 -v $(pwd)/devcerts:/mnt/certs --network=farm-os-development_default -it $(docker build -q src/) --farm-os-url=http://www:80 --proxy-spec="ssl:5707:privateKey=/mnt/certs/key.pem:certKey=/mnt/certs/cert.pem"
```

*Note: Don't forget to register the mkcert root CA in the QGIS settings if you want this to work reliably in QGIS.*

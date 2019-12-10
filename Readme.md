# farm-os-area-feature-proxy

farm-os-area-feature-proxy is a stand-alone proxy which makes a standard FarmOS installation accessible as a Web Feature Service (WFS) which can be used in programs such as Quantum GIS.

## Getting Started

```bash
docker run --name=farm-os-area-feature-proxy --rm -p 5707:5707 -it $(docker build -q src/) --farm-os-url=http://172.17.0.2:123
```

Or when running against the [FarmOS development docker-compose](https://farmos.org/development/docker/) environment;

```bash
docker run --name=farm-os-area-feature-proxy --rm -p 5707:5707 --network=farm-os-development_default -it $(docker build -q src/) --farm-os-url=http://www
```

## Future Work

* Update to use actual python API rather than the geojson endpoints
* Implement editing
* Implement caching of FarmOS login sessions
* Improve error handling
* Enable https
* Support GeometryCollection features

## FAQ

### Doesn't FarmOS already support mapping directly?

Yes, but it's convenient to be able to use the FarmOS data directly in fully-fledged GIS tools without importing/exporting the data or allowing direct DB access.

### Why write a stand-alone proxy? Wouldn't it be better to build the WFS server functionality directly into FarmOS?

Possibly, but it would be more costly to get this proof-of-concept into a state where it could be a reasonable pull-request. Also, there are advantages in not bloating FarmOS with functionality that only some users will need.
##
# symbioquine/farm-os-area-feature-proxy

# This file is available at the option of the licensee under:
# Public domain
# or licensed under X/MIT (LICENSE.TXT) Copyright 2019 Symbioquine <symbioquine@gmail.com>

FROM osgeo/gdal:alpine-normal-latest as builder

MAINTAINER Symbioquine <symbioquine@gmail.com>

# Based on https://github.com/gsemet/docker-alpine-s6-python3-twisted/blob/master/Dockerfile
RUN \
apk add --no-cache --virtual=build-dependencies \
    autoconf \
    automake \
    freetype-dev \
    g++ \
    gcc \
    jpeg-dev \
    lcms2-dev \
    libffi-dev \
    libpng-dev \
    libwebp-dev \
    libxml2-dev \
    libxslt-dev \
    linux-headers \
    make \
    openjpeg-dev \
    openssl-dev \
    python3-dev \
    tiff-dev \
    zlib-dev && \

apk add --no-cache --virtual=run-dependencies \
    libxml2 \
    libxslt && \

pip3 install --no-cache-dir -U \
    # cheetah \ => not compatible with python 3
    'twisted==17.9.0' \
    'lxml' \
    'cachetools' \
    'pyopenssl' \
    'semantic_version' \
    'service_identity' && \

# clean up
apk del --purge \
    build-dependencies && \
rm -rf \
    /root/.cache \
    /tmp/*

COPY . /app
WORKDIR /app

EXPOSE 5707

ENTRYPOINT ["python3", "fosafp.py"]

FROM arm64v8/postgres:17.11-alpine@sha256:1108a8fe22dff4eab48870e081be046d549ddc1b6f5972e0a766eaf1e0b1003c AS build
RUN apk add --no-cache build-base cmake openssl-dev icu-dev curl
RUN set -eu; \
    curl -fSL --retry 3 https://codeload.github.com/timescale/timescaledb/tar.gz/refs/tags/2.29.1 -o /tmp/timescaledb.tar.gz; \
    echo 'e09098955d328caa99bacaf68142b6348bb3b0ea07d5fa646d8bc3c077abc3c1  /tmp/timescaledb.tar.gz' | sha256sum -c -; \
    mkdir /tmp/timescaledb; \
    tar -xzf /tmp/timescaledb.tar.gz -C /tmp/timescaledb --strip-components=1; \
    cmake -S /tmp/timescaledb -B /tmp/timescaledb/build \
      -DCMAKE_BUILD_TYPE=Release -DREGRESS_CHECKS=OFF -DTAP_CHECKS=OFF -DWARNINGS_AS_ERRORS=OFF; \
    cmake --build /tmp/timescaledb/build --parallel 2; \
    DESTDIR=/stage cmake --install /tmp/timescaledb/build
FROM arm64v8/postgres:17.11-alpine@sha256:1108a8fe22dff4eab48870e081be046d549ddc1b6f5972e0a766eaf1e0b1003c
COPY --from=build /stage/ /

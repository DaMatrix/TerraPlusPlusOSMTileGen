#
# BASE IMAGE
#

FROM debian:bullseye AS base

#configure apt-get
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean autoclean \
    && rm -rf /var/lib/apt/lists/* /var/lib/apt/* /var/log/apt/* \
    && rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

#make apt-get go through eatmydata for Speed:tm:
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y eatmydata \
    && ln -s /usr/bin/eatmydata /usr/local/bin/apt-get \
    && ln -s /usr/bin/eatmydata /usr/local/bin/dpkg

# wget and curl
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y wget curl

# add adoptium repo
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    mkdir -p /etc/apt/keyrings \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list

#
# COMPILE T++OSMTileGen
#

FROM base AS compile

# openjdk-8-jdk
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y temurin-8-jdk
ENV JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64

# make, clang++, lld
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y make clang lld

# compile T++OSMTileGen
RUN --mount=target=/tmp/tpposmtilegen-src,rw --mount=type=cache,target=/root/.gradle \
    cd /tmp/tpposmtilegen-src \
    && ./gradlew test installDist \
    && ldd src/main/resources/net/daporkchop/tpposmtilegen/natives/x86_64-linux-gnu.so \
    && mkdir /tpposmtilegen \
    && mv build/install/T++OSMTileGen /tpposmtilegen/tpposmtilegen \
    && mv build/native-deps/shared/librocksdbjni-linux64.so /tpposmtilegen

#
# RUNTIME DEPENDENCIES AND CONFIGURATION
#

FROM base AS run

# openjdk-8-jre
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y temurin-8-jre
ENV JAVA_HOME=/usr/lib/jvm/temurin-8-jre-amd64

# openmp, tcmalloc
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
    apt-get update \
    && apt-get install -y libomp5-11 libtcmalloc-minimal4
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libtcmalloc_minimal.so.4

# create user-writable log directory
RUN mkdir /logs && chmod o+rw /logs

# copy binaries and librocksdbjni-linux64.so from compile image
COPY --link --from=compile /tpposmtilegen /usr/lib
ENV PATH="/usr/lib/tpposmtilegen/bin:${PATH}"

ENV JAVA_OPTS="-Xmx1G -Xms256M -XX:+UseConcMarkSweepGC"
ENTRYPOINT ["T++OSMTileGen"]

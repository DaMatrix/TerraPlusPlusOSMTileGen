FROM debian:bullseye

#configure apt-get
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean autoclean \
    && rm -rf /var/lib/apt/lists/* /var/lib/apt/* /var/log/apt/* \
    && rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

#make apt-get go through eatmydata for Speed:tm:
#RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
RUN \
    apt-get update \
    && apt-get install -y eatmydata \
    && ln -s /usr/bin/eatmydata /usr/local/bin/apt-get \
    && ln -s /usr/bin/eatmydata /usr/local/bin/dpkg

# wget
#RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
RUN \
    apt-get update \
    && apt-get install -y wget

# openjdk-8-jre-headless
#RUN --mount=type=cache,target=/var/cache/apt,sharing=locked --mount=type=cache,target=/var/lib/apt,sharing=locked --mount=type=tmpfs,target=/var/log/apt \
RUN \
    mkdir -p /etc/apt/keyrings \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y temurin-8-jdk

RUN \
    apt-get update \
    && apt-get install -y librocksdb-dev

COPY librocksdbjni-linux64.so /usr/lib

ENV JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
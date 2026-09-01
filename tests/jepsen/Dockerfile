FROM clojure:temurin-21-lein-2.13.0-noble@sha256:4d3b46b4b6d0aa29c03e819cdc814c993c43d75ec77e3d16616a0ba9bc9960c7

ARG USE_ALIYUN_MIRRORS=true

RUN if [ "$USE_ALIYUN_MIRRORS" = true ]; then \
        sed -i \
            -e 's|http://archive.ubuntu.com/ubuntu/|https://mirrors.aliyun.com/ubuntu/|g' \
            -e 's|http://security.ubuntu.com/ubuntu/|https://mirrors.aliyun.com/ubuntu/|g' \
            /etc/apt/sources.list.d/ubuntu.sources; \
    fi \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker.io git gnuplot-nox librocksdb8.9 \
    && rm -rf /var/lib/apt/lists/*

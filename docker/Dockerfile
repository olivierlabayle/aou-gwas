FROM debian:trixie

RUN apt-get clean \
    && apt-get update \
    && apt-get install -y curl

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin/:$PATH"

COPY . /opt/aou-gwas

WORKDIR /opt/aou-gwas

RUN uv sync

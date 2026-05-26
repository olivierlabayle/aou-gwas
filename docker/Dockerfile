FROM debian:trixie

RUN apt-get clean \
    && apt-get update \
    && apt-get install -y curl

# Install PLINK2
RUN wget https://s3.amazonaws.com/plink2-assets/alpha6/plink2_linux_x86_64_20250129.zip \
    && unzip plink2_linux_x86_64_20250129.zip \
    && mv plink2 /usr/local/bin \
    && chmod +x /usr/local/bin/plink2

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

ENV PATH="/root/.local/bin/:$PATH"

COPY . /opt/aou-gwas

WORKDIR /opt/aou-gwas

RUN uv sync

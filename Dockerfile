# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.11.23 AS uv

FROM ubuntu:22.04

ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH} \
    PATH=/opt/venv/bin:${PATH} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/python

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        openjdk-17-jre-headless \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/

RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --create-home --shell /bin/bash app \
    && mkdir -p \
        /home/app/.ivy2 \
        /var/lib/crypto-dashboard/checkpoints \
        /workspace \
    && chown -R app:app \
        /home/app/.ivy2 \
        /var/lib/crypto-dashboard \
        /workspace

WORKDIR /workspace

COPY --chown=app:app pyproject.toml uv.lock .python-version ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv python install 3.12 \
    && uv sync --locked --no-dev --no-install-project

COPY --chown=app:app . .

USER app

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "crypto-dashboard/app.py"]

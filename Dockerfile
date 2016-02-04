FROM alpine:3.3
COPY setup.py /azsync/
RUN apk add --update python libstdc++ python-dev py-setuptools ca-certificates build-base && \
    mkdir /azsync/azsync && touch /azsync/azsync/__init__.py && \
    cd azsync && python setup.py develop -v && rm -rf /azsync && \
    apk del --purge python-dev py-setuptools ca-certificates build-base && rm -rf /var/cache/apk/*
COPY azsync/__main__.py azsync/__init__.py azsync/test.py /azsync/
ENTRYPOINT python -m azsync
CMD --all

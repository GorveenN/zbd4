#!/bin/bash
# docker run -p 5432:5432  -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker run -p 5432:5432 --cpus="1.0" --memory="512m" -e POSTGRES_HOST_AUTH_METHOD=trust postgres


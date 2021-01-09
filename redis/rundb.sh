#!/bin/bash
docker run -p 6379:6379 --cpus="1.0" --memory="512m" -e ALLOW_EMPTY_PASSWORD=yes redis:latest

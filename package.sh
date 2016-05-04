#!/usr/bin/env bash

rm -rf pkg
mkdir pkg
python -m zipfile -c pkg/starfuse starfuse __main__.py

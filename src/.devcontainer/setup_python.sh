#!/usr/bin/env bash

cd $HOME/workspace && \
python -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install setuptools && \
pip install -r $HOME/workspace/.devcontainer/requirements.txt
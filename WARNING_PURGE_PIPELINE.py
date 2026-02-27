#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Legacy wrapper for CLI purge.
Prefer: moldock purge [PROJECT_DIR]
"""

from pathlib import Path

from moldockpipe.purge import purge_project


if __name__ == "__main__":
    purge_project(Path("."))

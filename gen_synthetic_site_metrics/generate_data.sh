#!/bin/bash

set -e

# For the moment we use this temporary hack to generate the sites' data.
python3 gen_synthetic_site_metrics/gen_synthetic_site_metrics.py sites_data/sites_latlngpue_reduced.json sites_data/sites_loc_metrics.json
#!/bin/bash
#
# Build master branch documentation
# Passing 'all' as first argument will also generate documentation for each stable branch
# based on listing the config files for Jekyll (_config.*.yaml)
# The version should be correspond to lakeFS `stable/$version` branch.
#

set -e

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd $script_dir/..
bundle install

echo "Jekyll build for master"
bundle exec jekyll build --config _config.yml -d _site
echo "Jekyll build for master completed successfully"

[ "$1" != "all" ] && exit 0

for cfg in _config.*.yml; do
    version=$(basename $cfg .yml | cut -c9-)
    echo "Jekyll build for $version"
    git checkout stable/$version
    bundle exec jekyll build --config _config.yml,$cfg -d _site/$version
    echo "Jekyll build for $version completed successfully"
done 


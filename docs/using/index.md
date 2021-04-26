---
layout: default
title: Using lakeFS with...
description: You can use lakeFS with all modern data frameworks such as Spark, Hive, AWS Athena, and Presto, and with your existing SDKs for all major programming languages
nav_order: 35
has_children: true
has_toc: false
---

lakeFS fits into your existing storage and compute infrastructure.

# Use lakeFS with these...

## Client applications

{%- assign pg = site.pages | sort_natural: "nav_order" %}

{%- for page in pg %}
  {%- if page.tags contains 'using/client-apps' %}
* [{{ page.title }}]({{ page.url | relative_url}})
  {%- endif %}
{%- endfor %}

## Software-as-a-Service clients

{%- for page in pg %}
  {%- if page.tags contains 'using/saas-apps' %}
* [{{ page.title }}]({{ page.url | relative_url}})
  {%- endif %}
{%- endfor %}

## SDKs and command-line tools

{%- for page in pg %}
  {%- if page.tags contains 'using/sdks' %}
* [{{ page.title }}]({{ page.url | relative_url}})
  {%- endif %}
{%- endfor %}

# Playground

## Requirements
1. Provide users a place where they can try out lakeFS without the need to install or run anything.
2. Support running lakectl (or any other way to run lakeFS commands)
3. Support inserting, ingesting and querying data through lakeFS (with tools such as Spark, Presto...).
4. Provide examples following the recommended branch model


## Non-Requirements
1. Provide cloud service or storage of any kind
2. Provide an SLA for running spark/presto on lakeFS
3. Explain how to install start and config lakeFS

## Solution

### Katacoda 
Katacoda is an interactive technology platform that enables hands-on learning.
we will use Katacoda to create a few step by step tutorials including: 
1. lakectl commands - versioning 101 (repo creation and listing, commit commands, fs commands , diff commands) 
2. Running spark job on a branch and merging back
3. Querying history with Presto.

### Tutorial
In every tutorial the user will get a working environment with all the required tools installed.
The user will follow a step by step flow, and will be able to run the commands and see the results.
The environment is a real environment allowing the user to play around with other commands of their choice.


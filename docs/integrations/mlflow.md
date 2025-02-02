---
title: MLflow
description: How to use MLflow with lakeFS
parent: Integrations
---

# Using MLflow with lakeFS

[MLflow](https://mlflow.org/docs/latest/index.html) is tool for managing the ML lifecycle, built to assist machine learning
practitioners and teams in handling the complexities of the machine learning process. It focuses on the full lifecycle 
for machine learning projects, ensuring that each phase is manageable, traceable, and reproducible.

MLflow is built of multiple core component, and lakeFS seamlessly integrates with the [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html#tracking) 
component which enables experiment tracking that takes into account experiment inputs and outputs and enables visualizing 
experiment and experiment comparison results.


[MLflow](https://mlflow.org/docs/latest/index.html) is a comprehensive tool designed to manage the machine learning lifecycle,
assisting practitioners and teams in handling the complexities of ML processes. It focuses on the full lifecycle of machine
learning projects, ensuring that each phase is manageable, traceable, and reproducible.

MLflow comprises multiple core components, and lakeFS seamlessly integrates with the [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html#tracking)
component. MLflow tracking enables experiment tracking that accounts for both inputs and outputs, allowing for visualization
and comparison of experiment results.

{% include toc_2-3.html %}

## Why use MLflow with lakeFS? 

1. **Experiment Reproducibility**: As a data versioning system, lakeFS enables dataset versioning. Combined with MLflow's
[input logging](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_input) capability, lakeFS helps you track
not only the dataset used for an experiment run but its exact version. Accurate tracking of run inputs makes experiments
truly reproducible.
2. **Parallel Experiments with Zero Data Copy**: lakeFS employs a copy-on-write technique, allowing for efficient 
[branch](../understand/model.md#branches) creation without duplicating data. This enables multiple experiments to be 
conducted in parallel, with each branch providing an isolated environment for modifications. Changes made in one branch
do not affect others, ensuring safe collaboration among team members. Once an experiment is complete, the branch can be
merged back into the main dataset, incorporating the new insights seamlessly.

## How to use MLflow with lakeFS

We will demonstrate how to use lakeFS to load versioned data into MLflow experiment runs, log run inputs, and later trace
back the exact dataset used for a run to reproduce experiment results.

### Loading versioned datasets


### Python libraries integration  

### Spark-based 

### Logging inputs

By leveraging MLflow's input logging capabilities, you can record the exact version of the dataset used in each experiment
run. This involves loading from a specific lakeFS branch or commit corresponding to the dataset version, ensuring precise tracking 
of experiment inputs.

### Inspecting runs input  

MLflow's tracking UI allows you to inspect the inputs of each run, including the specific dataset logged. However, to 
inspect the exact dataset version we recommend the following approach.  




By integrating lakeFS with MLflow, you can enhance your machine learning workflows with robust data versioning, ensuring 
that your experiments are reproducible and your collaborations are efficient.



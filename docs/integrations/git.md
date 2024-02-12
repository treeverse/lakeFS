---
title: Git
description: This section explains how to integrate lakeFS with Git
parent: Integrations
---

# Integrating lakeFS with Git

Integrating code and data version control systems allows you to associate code versions with data versions, facilitating
the reproduction of complex environments with multiple components. Consequently, lakeFS, a data version control system, 
seamlessly integrates with Git. This combination establishes a robust foundation for versioning both your code and data, 
fostering a streamlined and reproducible development process.

## Use Cases

### Develop Reproducible ML Models

Maintain a comprehensive record of both model code versions and input data versions to ensure the reproducibility of ML 
model results.

The common way to develop reproducible ML models with lakeFS is to use the 
[lakectl local](../reference/cli.md#lakectl-local) command. See [_Working with lakeFS Data Locally_](../howto/local-checkouts.md#example-using-lakectl-local-in-tandem-with-git) 
to understand how to use lakectl local in conjunction with Git to develop reproducible ML models.    

### Develop Reproducible ETL Pipelines

Track code versions for each step in ETL pipelines along with the corresponding data versions of their inputs and outputs.
This approach allows straight forward troubleshooting and reproduction of data errors. 

Check out [this](https://github.com/treeverse/lakeFS-samples/tree/main/01_standalone_examples/airflow-02) lakeFS sample 
that demonstrates how Git, Airflow, and lakeFS can be integrated to result in reproducible ETL pipelines.   


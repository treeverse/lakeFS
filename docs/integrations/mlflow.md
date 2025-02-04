---
title: MLflow
description: How to use MLflow with lakeFS
parent: Integrations
---

# Using MLflow with lakeFS

[MLflow](https://mlflow.org/docs/latest/index.html) is a comprehensive tool designed to manage the machine learning lifecycle,
assisting practitioners and teams in handling the complexities of ML processes. It focuses on the full lifecycle of machine
learning projects, ensuring that each phase is manageable, traceable, and reproducible.

MLflow comprises multiple core components, and lakeFS seamlessly integrates with the [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html#tracking)
component. MLflow tracking enables experiment tracking that accounts for both inputs and outputs, allowing for visualization
and comparison of experiment results.

{% include toc_2-3.html %}

## Benefits of integrating MLflow with lakeFS 

Integrating MLflow with lakeFS offers several advantages that enhance the machine learning workflow:
1. **Experiment Reproducibility**: By leveraging MLflow's [input logging](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_input)
capabilities alongside lakeFS's data versioning, you can precisely track the specific dataset version used in each experiment
run. This ensures that experiments remain reproducible over time, even as datasets evolve.
2. **Parallel Experiments with Zero Data Copy**: Parallel Experiments with Zero Data Copy: lakeFS enables efficient [branching](../understand/model.md#branches) without
duplicating data. This allows for multiple experiments to be conducted in parallel, with each branch providing an isolated
environment for dataset modifications. Changes in one branch do not affect others, promoting safe collaboration among
team members. Once an experiment is complete, the branch can be seamlessly merged back into the main dataset, incorporating
new insights.

## How to use MLflow with lakeFS

To harness the combined capabilities of MLflow and lakeFS for safe experimentation and accurate result reproduction, consider
the workflow below and review the [practical examples](#practical-examples) provided on the next section.  

1. **Create a branch for each experiment**: Start each experiment by creating a dedicated lakeFS branch for it. This approach 
allows you to safely make changes to your input dataset without duplicating it. You will later load data from this branch 
to your MLflow experiment runs. 
2. **Read datasets from the experiment branch**: Read Datasets from the Experiment Branch: Conduct your experiments by 
reading data directly from the dedicated branch. We recommend to read the dataset from the head commit of the branch to
ensure precise version tracking.
3. **Create an MLflow Dataset pointing to lakeFS**: Use MLflow's [Dataset](https://mlflow.org/docs/latest/python_api/mlflow.data.html#mlflow.data.dataset.Dataset)
ensuring that the [dataset source](https://mlflow.org/docs/latest/python_api/mlflow.data.html#mlflow.data.dataset_source.DatasetSource)
points to lakeFS. 
4. **Log your input**: Use MLflow's [log_input](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_input) 
function to log the versioned dataset stored in lakeFS.    
5. **Commit dataset changes**: Machine learning development is inherently iterative. When you make changes to your input dataset,
commit them to the experiment branch in lakeFS with a meaningful commit message. During an experiment run, load the dataset
version corresponding to the branch's head commit and track this reference to facilitate future result reproduction.
6. **Merge experiment results**: After concluding your experimentation, merge the branch used for the selected experiment 
run back into the main branch.

{: .note}

**Note: Branch per experiment Vs. Branch per experiment run**<br>
While it's possible to create a lakeFS branch for each experiment run, given that lakeFS branches are both quick and 
cost-effective to create, it's often more efficient to create a branch per experiment. By reading directly from the head
commit of the experiment branch, you can distinguish between dataset versions without creating excessive branches. This
practice maintains branch hygiene within lakeFS.

#### Load versioned datasets

mlflow.data provides APIs for constructing Datasets from a variety of Python data objects, Spark DataFrames, and more. 
lakeFS seamless integration with both Spark and common Python libraries enables creating MLflow 
[Datasets](https://mlflow.org/docs/latest/python_api/mlflow.data.html#mlflow.data.dataset.Dataset) with pointing to 
[Dataset source](https://mlflow.org/docs/latest/python_api/mlflow.data.html#mlflow.data.dataset_source.DatasetSource) on
lakeFS. 

* s3 gateway only? 

### Practical examples

#### Example: Using Pandas 

```python
import lakefs 
import mlflow

repo = lakefs.Repository("mlflow-tracking").create(storage_namespace="bucket/my-namespace", default_branch="main", exist_ok=True)
repo_id = repo.id

exp_branch = repo.branch("experiment-1").create(source_reference="main", exist_ok=True)
branch_id = exp_branch.id
head_commit_id = exp_branch.head.id

table_path = "famous_people.csv"

dataset_source_url = f"s3://{repo_id}/{head_commit_id}/{table_path}"

# Use Pandas to read from lakeFS, at its most updated version to which the head commit id is pointing
raw_data = pd.read_csv(dataset_source_url, delimiter=";", storage_options={
        "key": "AKIAIOSFOLKFSSAMPLES",
        "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "client_kwargs": {"endpoint_url": "http://lakefs:8000"}
    })

# Create an instance of a PandasDataset
dataset = mlflow.data.from_pandas(
    raw_data, source=dataset_source_url, name="famous_people"
)

# View some of the recorded Dataset information
print(f"Dataset name: {dataset.name}")
print(f"Dataset source URI: {dataset.source.uri}")


# Use mlflow input logging to track the dataset versioned by lakeFS
with mlflow.start_run() as run:
    mlflow.log_input(dataset, context="training")
    mlflow.set_tag("lakefs_repo", repo_id)
    mlflow.set_tag("lakefs_branch", branch_id) # Log the branch id, to have a friendly lakeFS reference to search the input dataset in 


# Inspect run's dataset
logged_run = mlflow.get_run(run.info.run_id) # 

# Retrieve the Dataset object
logged_dataset = logged_run.inputs.dataset_inputs[0].dataset

# View some of the recorded Dataset information
print(f"Dataset name: {logged_dataset.name}")
print(f"Dataset source URI: {logged_dataset.source}")
```

Output
```text
Dataset name: famous_people
Dataset source URI: s3://mlflow-tracking/3afddad4fef987b4919f5e82f16682c018f59ed2ff003a6a81adf72edaad23c3/fp.csv
Dataset name: famous_people
Dataset source URI: {"uri": "s3://mlflow-tracking/3afddad4fef987b4919f5e82f16682c018f59ed2ff003a6a81adf72edaad23c3/fp.csv"}
```

##### Limitations

* can't use lakeFS spec, only for loading data but not for ceating mlflow datasets. This will be supported when we have
  an MLflow dataset source

#### Example: Using Spark

##### Configuration

To configure lakeFS to load a Spark dataframe or multiple formats, including Delta Lake tables, configure Spark to work 
with lakeFS [S3-compatible API](spark.md#s3-compatible-api), and Delta Lake as follows: 

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lakeFS / Mlflow") \
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", 'https://example-org.us-east-1.lakefscloud.io') \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.access.key", 'AKIAlakefs12345EXAMPLE') \
    .config("spark.hadoop.fs.s3a.secret.key", 'abc/lakefs/1234567bPxRfiCYEXAMPLEKEY') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```


```python
import lakefs 
import mlflow

repo = lakefs.Repository("mlflow-tracking").create(storage_namespace="bucket/my-namespace", default_branch="main", exist_ok=True)
repo_id = repo.id

exp_branch = repo.branch("experiment-1").create(source_reference="main", exist_ok=True)
branch_id = exp_branch.id
head_commit_id = exp_branch.head.id

table_path = "gold/train_v2/"

dataset_source_url = f"s3://{repo_id}/{head_commit_id}/{table_path}"

# Load delta lake table from lakeFS, at its most updated version to which the head commit id is pointing
dataset = mlflow.data.load_delta(path=dataset_source_url, name="boat-images")

# View some of the recorded Dataset information
print(f"Dataset name: {dataset.name}")
print(f"Dataset source URI: {dataset.source.path}")

# Use mlflow input logging to track the dataset versioned by lakeFS
with mlflow.start_run() as run:
    mlflow.log_input(dataset, context="training")
    mlflow.set_tag("lakefs_repo", repo_id)
    mlflow.set_tag("lakefs_branch", branch_id) # Log the branch id, to have a friendly lakeFS reference to search the input dataset in 

# Inspect run's dataset
logged_run = mlflow.get_run(run.info.run_id) # 

# Retrieve the Dataset object
logged_dataset = logged_run.inputs.dataset_inputs[0].dataset

# View some of the recorded Dataset information
print(f"Dataset name: {logged_dataset.name}")
print(f"Dataset source URI: {logged_dataset.source}")
```

Output 
```text
Dataset name: boat-images
Dataset source URI: s3://mlflow-tracking/3afddad4fef987b4919f5e82f16682c018f59ed2ff003a6a81adf72edaad23c3/gold/train_v2/
Dataset name: boat-images
Dataset source URI: {"path": "s3://mlflow-tracking/3afddad4fef987b4919f5e82f16682c018f59ed2ff003a6a81adf72edaad23c3/gold/train_v2/"}
```

![Run inspection](../assets/img/mlflow_inspect_experiment_run.png)

```python
import mlflow

# Inspect run's dataset and tags
run = mlflow.get_run("c0f8fbb1b63748abaa0a6479115e272c") # 

# Retrieve the Dataset object
logged_dataset = run.inputs.dataset_inputs[0].dataset

# View some of the recorded Dataset information
print(f"Dataset name: {logged_dataset.name}")
print(f"Dataset source URI: {logged_dataset.source}")

# Retrieve run's tags 
logged_tags = run.data.tags
print(f"Run tags: {logged_tags}")
```

```text
Dataset name: boat-images
Dataset source URI: {"path": "s3://mlflow-tracking/3afddad4fef987b4919f5e82f16682c018f59ed2ff003a6a81adf72edaad23c3/gold/train_v2/"}
Run tags: {'lakefs_branch': 'experiment-1', 'lakefs_repo': 'mlflow-tracking'}
```

**Note:** 
The URI schema is s3 because we configured lakeFS to use Spark via the s3 gateway. with these configurations the `lakefs://`
schema won't work. 


### Reproducing experiment results  

#### Inspecting runs input  

MLflow's tracking UI allows you to inspect the inputs of each run, including the specific dataset logged. However, to 
inspect the exact dataset version we recommend the following approach.  

1. Get runID: go to the MLflow UI and copy the runID of your choice.
2. Extract dataset information: extract the information of the dataset used in this run.  

```python
# Retrieve the run information
logged_run = mlflow.get_run("5f7c01e28b2e41b0963dab99198f278f")

# Retrieve the Dataset object
logged_dataset = logged_run.inputs.dataset_inputs[0].dataset

# View some of the recorded Dataset information
print(f"Dataset name: {logged_dataset.name}")
print(f"Dataset digest: {logged_dataset.digest}")
print(f"Dataset source URI: {logged_dataset.source}")
```
Output 
```text
## Output
Dataset name: boat-images
Dataset digest: e88c85ce
Dataset source URI: {"path": "s3://repo/experiment-branch/gold/train_v2/"}
```


* You can assert if two experiment runs used the same dataset by comparing dataset sources. 

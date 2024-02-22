# 1. Project overview
## What is this project about?
The goal of this project is to define a template for a classic ETL and ML training pipeline with Kubeflow.


# 2. Usage of ML Pipeline
These steps has to be conducted via the Kubeflow UI in order to run the pipeline:
1. Navigate to Kubeflow GUI
2. Select the pipeline
3. Select the latest pipeline version
4. Enter a pipeline name of your choice
5. Select an experiment of your choice
6. Enter pipeline parameters (=data scope)*:
   - param_1: Explanation

*All parameters are passed as string automatically and do not need ' ' or " "

# 3. Software Structure
## 3.1 Code in Github repository

```
~/
|-- github/ - GitHub workflows and actions to deploy the ML pipeline, and run tests
|-- config/ - use-case-specific configuration
|-- data/ - files with the structure of the result data
|-- ml_pipeline/ - Python modules for each pipeline step that are implemented through Kubeflow
|-- notebooks/ - Executable Jupyter Notebooks
|-- tests/ - Unit & integration tests
|-- pyproject.toml - package and dependency specification for the `ml_pipeline` module
```
The code is structured in a processual way. The _pipeline.py_ inside _ml_pipeline/_ represents the ML pipline.
This file defines the execution plan of the pipeline. When this file is executed, the entire code of the pipeline is
loaded and transmitted to the Kubeflow service. The pipeline steps are organised as modules within _ml_pipeline/components/_.
Each python module contains of a _<pipeline step name>.py_ (=execution order of pipeline step) and a _steps.py_ (=implementation of functions).

```
ml_pipeline(<data scope param>):
   setup_extraction_op(config)

   glue_op(config) (for data extraction)

   setup_pipeline_op(config)

   preprocessing_op(config)

   feature_engineering_op(config)

   set_mining_op(config)
```

## 3.2 Data in S3
Data that is processed by the pipeline is stored in S3. We have:
- __pipeline_input__: Static files and Glue Job data extractions.
- __pipeline_tmp__: Every pipeline step needs input data loaded from S3 and uploads its processed data to S3. No data is directly sent to the next pipeline step. If the whole pipeline was executed, this data will be deleted.
- __pipeline_outputs__: The most frequent error sets - as the result of the ML algorithm - as well as kpis of each pipline steps and relevant data of intermediat pipeline results.

We have three 'data domains':
- __kubeflow__: This directory is used when the Kubeflow Pipeline is executed.
- __playground__: This directory is used when the Jupyter Notebooks are used for exploration tasks.
- __test_data__: This directory is used when the integration test is executed.


### 4. **Setup virtual environment**
Navigate to the project repository and execute the following commands:
    poetry install --with test
    poetry shell


# 5. Contribute to this software
## I. Python Code
Use notebooks in _notebooks/_ for exploration and experiment purpose. If you want to integrate new code into the
pipeline, then add it to the corresponding python module and use this Jupyter Notebook for debugging:
_notebooks/debug_pipeline.ipynb_

## II. Spark Code
You should develop and run code that contains pyspark in a Jupyter Notebook based on the jupyter-glue:1.5.0 image.

# 6. Deploying the Pipeline
## I. CI/CD
### Prerequisites

1. **AWS credentials as GitHub secrets**
- Make sure to include the `AWS AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` as secrets in your GitHub repository settings (`Settings > Secrets`). You can access the values of these secrets in the AWS Secrets Manages, under the names:
	- `/third-party/<YOUR-PRODUCT-NAME>.<YOUR-PROFILE>/aws-access-key-id`
	- `/third-party/<YOUR-PRODUCT-NAME>.<YOUR-PROFILE>/aws-secret-access-key`

2. **GitHub Runners**
- Make sure your GitHub organization has the appropriate GitHub runners provisioned by the Connected AI team. For that, you can check if the runners are available under the repository settings:
	- `Settings > GitHub apps`

#### Usage

In principle, the set-up ML pipeline GitHub workflow should get triggered on push.

## II. Code Server
#### Usage

- You can use this command to upload your local code base to Kubeflow:
  ```shell
  python -m ml_pipeline.pipeline --type update
  ```

	* NOTE: Under the hood, this will generate a .zip with `pipeline.yaml` and update the pipeline in Kubeflow.


# 6. Tests

### Run Unit Tests

- On the project root, run via Terminal:
  ```shell
  python -m pytest tests/unit_tests/ -v -p no:warnings
  ```

### Run Integration Tests

- On the project root, run:
  ```shell
  pytest tests/integration_test/ -v -p no:warnings
  ```
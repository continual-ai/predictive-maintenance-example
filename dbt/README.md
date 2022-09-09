Here you will find the dbt project used by the Continual team to build out the predictive maintenance use case for our Azure VM example. 

### Using the project

To get started, refer to our guided example on [predictive maintenance](https://docs.continual.ai/predictive-maintenance/) for a more detailed walkthrough. 

To get started with this dbt project, run the following commands (this project utilizes dbt-utils):

```sh
dbt deps
dbt run
dbt test
```

If you want to check out the project documentation: 

```sh
dbt docs generate
dbt docs serve
```

Once the dbt project has executed, you can create corresponding models and feature sets in Continual by running the following: 

```sh
continual run
```

*Note: Make sure that you have the [Continual CLI](https://docs.continual.ai/installing/) installed and that you have signed in with a selected default project*

### Resources:

This dbt project is meant to complement Continual's [guided example for predictive maintenance](https://docs.continual.ai/predictive-maintenance/).
- This dbt project was built using the following [Microsoft Azure Predictive Maintenance](https://www.kaggle.com/datasets/arnabbiswas1/microsoft-azure-predictive-maintenance) datasets found through Kaggle
- More guided examples can be found by checking out [Continual's documentation](https://docs.continual.ai/)!

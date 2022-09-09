# predictive-maintenance-example

Welcome to Continual's predictive maintenance example, based on the following [Kaggle for Microsoft Azure Predictive Maintenance](https://www.kaggle.com/datasets/arnabbiswas1/microsoft-azure-predictive-maintenance). For a full walkthrough, please visit our [documentation](https://docs.continual.ai/predictive-maintenance/) and you can find more guided example [here](https://docs.continual.ai/examples-overview/). 

Note: This project is designed to run with Snowflake. You should be able to adapt it though pretty easily to other warehouse vendors, but [let us know](https://docs.continual.ai/help-support/) if you encounter any issues. 

## Running the example

Download the source data at Kaggle:
```
kaggle datasets download -d arnabbiswas1/microsoft-azure-predictive-maintenance 
```

You can then upload it to your cloud data warehouse of choice using your preferred mechanism. For convenience, we've included a few short scripts that you can leverage to upload this data into Snowflake manually. 

1. First, run the [ddl.sql](https://github.com/AndrewRTsao/predictive-maintenance-example/blob/main/sql/ddl.sql) file to create your base tables. 
2. Afterwards, using the CSVs downloaded from Kaggle, you can use `snowsql` to upload the data. Refer to the SnowSQL's [documentation](https://docs.snowflake.com/en/user-guide/snowsql-use.html#running-batch-scripts) and execute the [snowsql_staging.sql](https://github.com/AndrewRTsao/predictive-maintenance-example/blob/main/sql/snowsql_staging.sql) script through `snowsql`. 


## For dbt users

If you're using dbt, you'll now just be able to use the [dbt project](https://github.com/AndrewRTsao/predictive-maintenance-example/tree/main/dbt) provided. `dbt_project.yml` is configured to use the `continual` profile. You'll either need to change the profile accordingly or create this profile in your `~/.dbt/profiles.yml` file. Then you can execute:

 ```sh
 dbt run
 ```

This command will build all the required tables/views. Then, once dbt is finsihed, you can execute the following command to push the necessary configuration to Continual to kick off the model training process: 

```sh
continual run
```

Please make sure that you have already installed the Continual CLI, have an account created in Continual, and have logged in to the CLI with a default project configured. Otherwise, you will need to do so first before repeating the above process. 

You're now done! You can navigate to the Continual Web UI, either through the URL provided in the terminal output or by navigating to the Changes tab, to monitor the progress of your model training and observe the results as it finishes.

*Note: This whole process can take around 2 hours to finish.*

## For non-dbt users.

We highly recommend using dbt for your transformations. If this is not feasible, we've provided the following [feature_engineering.sql](https://github.com/AndrewRTsao/predictive-maintenance-example/blob/main/sql/feature_engineering.sql) and [prediction_engineering.sql](https://github.com/AndrewRTsao/predictive-maintenance-example/blob/main/sql/prediction_engineering.sql) SQL scripts that you can run in Snowflake directly to build out all required tables / views. 

Afterwards, from the command line, you can simply exeecute the following (from the directory of your locally cloned project):

```sh
continual push ./continual/featuresets ./continual/models
```

Please make sure that you have already installed the Continual CLI, have an account created in Continual, and have logged in to the CLI with a default project configured. Otherwise, you will need to do so first before repeating the above process. 

**Note**: If you modify any of the table names or schema in the .sql scripts, make sure to update the queries in the corresponding `.yaml` files in `continual/featuresets` and `continual/models` accordingly. Otherwise, you will encounter errors. 

You're now done! You can navigate to the Continual Web UI, either through the URL provided in the terminal output or by navigating to the Changes tab, to monitor the progress of your model training and observe the results as it finishes.

*Note: This whole process can take around 2 hours to finish.*

## Having issues?
Feel free to [contact us](https://docs.continual.ai/help-support) if you have any issues or open a PR directly with any suggested modifications.

Thank you for your time and we hope you enjoy this guided example!
This module rates the datasets based on various parameters as defined in [dataset_params.json](../dataset_params.json)
Few points about the params file are as follows. 
1. All the parameters are given equal priority for now. 
2. Priority is decided based on the value of _weight_ that each parameter has. 
3. If there are multiple levels in a parameter then the total contribution will be the product of weight, sub-weight and corresponding value in params. 
An example would make it clear. 
For format, the weight is 0.16. If the format is csv then the sub-weight is 1. So, if 100% of the resources are of csv type,
the total contribution would be 
                 `value of 100%(10) * weight of csv (1) * weight of formats(0.16) = 1.6`

HVD classifier is a prefect flow and can be deployed to trigger the run. The steps to deploy it are,
1. `prefect deployment build hvd_rating/rate_high_value_dataset.py:get_rating_and_update_dataset 
-n rate_hvd -t dev` - this would create a file by name - `get_rating_and_update_dataset.yaml`
2. Deploy this to cloud by running - `prefect deployment apply get_rating_and_update_dataset-deployment.yaml`
3. Start an agent in a terminal by running - `prefect agent start -t dev`
4. Go to the Prefect UI and run the flow listed in Deployments section.
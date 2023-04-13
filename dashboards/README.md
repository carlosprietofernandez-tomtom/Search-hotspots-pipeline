# Streamlit-flask
The template is structured as follows:

- [**apps:**](app.py) The folder with all the streamlit apps we are going to run.
- [**static:**](components) Folder with css and js.
- [**templates:**](components) Folder with the html template.
- [**Dockerfile:**](components) Dockerfile.
- [**app.py:**](components) Flask application that renders the html template to create a navbar.
- [**entrypoint.sh:**](components) Bash script to execute as entrypoint.


## Run locally
To run the application locally we simply need to execute
```
bash entrypoint.sh
```
within the streamlit-flask folder.

Once the command has been executed we should be able to find the application running on port 5000:

![Capture](https://user-images.githubusercontent.com/89970838/165403621-687a4830-457e-42ec-819b-d7557361ddc6.PNG)


## Production
To containerized the application and store the image in the azure container registry execute:
```
az acr build --registry strategicAnalysisAppRegistry --subscription maps-data-analytics-dev --resource-group StrategicAnalysis --image appname .
```

Finally, create a container instance with the uploaded image.

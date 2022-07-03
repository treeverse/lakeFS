# TimeToValue: Wizard MVP Design

## Description

This design document will specify a possible solution for the time-to-value [wizard MVP](https://github.com/treeverse/lakeFS/issues/3411).  
The wizard will provide a quick and clear way to start interacting with lakeFS.
It will do that by allowing the users to
1. Initialize a new repository with a given namespace (and name) with a ‘main’ default branch.
2. Import their data (write metadata) into the new repository’s main branch by specifying the data’s S3 bucket namespace.
3. Get custom Spark configurations and custom Hive metastore configurations to access lakeFS using the S3 Gateway (this is for the MVP).
4. Summarise all actions performed (or skipped) in a README file which will be at the root of the initialized repository.

---

## System Overview

![RouterFS with lakeFS URI](diagrams/wizard-mvp.png)
[(excalidraw file)](diagrams/wizard-mvp.excalidraw)

### Wizard UI Component

The wizard UI component is responsible for the user’s Spark onboarding process. The process is as follows:
1. Create a repository (named as the user wishes) and a ‘main’ branch in it.
2. Import the user’s data to ‘main’ and display a progress bar (which will show a link to required permissions). Only available on cloud-deployed lakeFS.
3. Generate Spark configurations: the wizard will return Spark configurations for the users to use in their Spark core-site.xml file, [databricks key-value format](https://docs.databricks.com/clusters/configure.html#spark-configuration), or [EMR JSON](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html) ([core-site](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html))
   -> All three templates will be requested (each in a separate request).
   1. Asks the user to enter the lakeFS endpoint (by default it will be https://`location.host`)
   2. [Figma Design](https://www.figma.com/file/haD9y599LzW6LvsYBI2xWU/Spark-use-case?node-id=31%3A200)
4. Generates a README file with all actions performed, and will commit it as a first commit (after the import) to the ‘main’ branch of the created repository.

### Templating Service

The [templating service](https://github.com/treeverse/lakeFS/pull/3373) is responsible for fetching, authenticating, and expanding the required templates and returning them to the client.  
**Process**:  
1. Get the template (the location should be specified in the incoming request). The file must be a valid [`html/template`](https://pkg.go.dev/html/template) (specified using the `.<type>.html.tt` suffix) or [`text/template`](https://pkg.go.dev/text/template) (specified using the `.<type>.tt` suffix) parsable template text.
2. Use the configured template functions to validate the user’s permissions to perform the required actions, and to generate credentials on the fly.
3. Expand the template with the config file and query string params, and return it with the correct `Content-Type` header (inferred from the template).

### Wizard Templates

The following templates will be saved within the lakeFS binary and be directed by the Wizard component to be expanded:
* SparkEMR.conf.tt
* SparkDatabricks.conf.tt
* SparkXML.conf.tt
* README.md.tt
* MetastoreEMR.conf.tt
* MetastoreDatabricks.conf.tt
* MetastoreXML.conf.tt

---

## APIs

### Templating Service

- **Endpoint**: `/api/v1/templates`  
- **Request**:
  - Method: `GET`
  - Parameters:
    - Template URL (`template_location`): `string` - URL of the template.  Retrieved from the query string, must be relative (to a URL configured on the server).
    - Any other configurations required for the templates: `string` - retrieved from query string.
- **Response**:
  - Return value: The expanded template
  - Headers:
    - `Content-Type` - The template's content type.
- **Errors**:
  1. *403- Forbidden*: The requesting user is forbidden from accessing the configurations or functionality (like generating credentials).
  2. *400- Bad request*: The request is missing information necessary for the template’s expansion.
  3. *500- Internal server error*: The lakeFS server cannot access some provided template locations.

---

## Possible Flows

### Example template

**Databricks Spark configurations**  
Name: *databricksConfig.props.tt*
```properties
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
{{with $creds := new_credentials}}
spark.hadoop.fs.s3a.access_key={{$creds.Key}}
spark.hadoop.fs.s3a.secret_key={{$creds.Secret}}
{{end}}
spark.hadoop.fs.s3a.endpoint={{ .querystring.lakefs_url }}
spark.hadoop.fs.s3a.path.style.access=true
```

**Local Metastore configurations**
Name: *localMetastoreConfig.xml.tt*
```xml
<configuration>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>{{ .querystring.lakefs_url }}</value>
    </property>
    {{with $creds := new_credentials}}
    <property>
        <name>fs.s3a.access.key</name>
        <value>{{$creds.Key}}</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>{{$creds.Secret}}</value>
    </property>
    {{end}}
</configuration>
```

### Happy flow - All steps

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to type the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user specifies the location of the bucket (after they validated that the lakeFS role has the right permissions and that the bucket has the correct policy), and clicks ‘OK’.
   * An object counter will show the progress of the import process and will signal once it’s over.
5. The wizard asks the user for their lakeFS endpoint (and will show a default placeholder pointing to the current URL).
6. The wizard will send a GET request to the templating service with a query string of the format:  
    ```
    ?lakefs_url=https://my-lakefs.io&template_location=databricksConfig.props.tt
    ```
7. The templating service will fetch the templates from the provided locations, expand the `.querystring.lakefs_url` parameter for both templates and return a response of the format:
    ```properties
    spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.access_key=ACCESSKEYDONTTELL
    spark.hadoop.fs.s3a.secret_key=SECRETKEYDONTTELL
    spark.hadoop.fs.s3a.endpoint=https://my-lakefs.io
    spark.hadoop.fs.s3a.path.style.access=true
    ```
   each returned template will return with a `Content-Type` header describing the way it should be presented.
8. The wizard will present each configuration in a different snippet view for the users to copy and paste into their configuration files.
9. The wizard will send a GET request to the templating service with a query string of the format:
    ```
    ?lakefs_url=https://my-lakefs.io&template_location=README.md.tt
    ```
   It will do so to generate a README.md file.
10. The returned README file will describe the steps taken, the configurations generated but without secrets and some commands to explain how to connect Hive Metastore to lakeFS:
     ```markdown
     1. Created a repository "spark-repo" and branch "main".
     2. Imported data from <S3 location>.
     3. Generated the following configurations:
        <Spark configurations with hidden credentials>
        <Metastore configurations with hidden credentials>
     4. Instructions to configure Hive Metastore with lakeFS.
     5. Generated this README file and committed it.
     ```
11. Upload and commit the README file to the `main` branch.

### Happy flow - Spark template only

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to type the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user skips this step using the skip button.
5. The wizard asks the user for their lakeFS endpoint (and will show a default placeholder pointing to the current URL).
6. The wizard will send a GET request to the templating service with a query string of the format:
    ```
    ?lakefs_url=https://my-lakefs.io&template_location=databricksConfig.props.tt
    ```
7. The templating service will fetch the templates from the provided locations, expand the `.querystring.lakefs_url` parameter for both templates and return a response of the format:
    ```properties
    spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.access_key=ACCESSKEYDONTTELL
    spark.hadoop.fs.s3a.secret_key=SECRETKEYDONTTELL
    spark.hadoop.fs.s3a.endpoint=https://my-lakefs.io
    spark.hadoop.fs.s3a.path.style.access=true
    ```
   each returned template will return with a `Content-Type` header describing the way it should be presented.
8. The wizard will present each configuration in a different snippet view for the users to copy and paste into their configuration files.
9. The wizard will send a GET request to the templating service with a query string of the format:
    ```
    ?lakefs_url=https://my-lakefs.io&template_location=README.md.tt
    ```
   It will do so to generate a README.md file.
10. The returned README file will describe the steps taken, the configurations generated but without secrets and some commands to explain how to connect Hive Metastore to lakeFS:
     ```markdown
     1. Created a repository "spark-repo" and branch "main".
     2. Generated the following configurations:
        <Spark configurations with hidden secrets>
     3. Instructions to configure Hive Metastore with lakeFS.
     4. Generated this README file and committed it.
     ```
11. Upload and commit the README file to the `main` branch.

### Sad flow - No import permissions

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to enter the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user specifies the location of the bucket, and clicks ‘OK’.
   1. The import functionality panics as there are no permissions to access the given storage.
   2. The wizard will show an error message like: “Please verify your lakeFS server and storage have the required permissions” and link to the docs to show the needed permissions.
5. Continue as above 
- The generated README will not include the import step.

### Sad flow - No credential generation permissions

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to enter the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user skips this step using the skip button.
6. The wizard sends a GET request to the templating service with a query string of the format:
    ```
    ?lakefs_url=<url>&template_location=databricksConfig.props.tt
    ```
7. The templating service will fetch the template from the provided location and will fail to generate the user’s credentials as the requesting user doesn’t have the permissions required.
8. The templating service will return **‘403 Forbidden’** to the wizard.
9. The wizard will prompt a message saying that the user doesn’t have the required permissions for generating credentials.
10. Continue with the flow as described above…

### Sad flow - Missing template properties

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to enter the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user skips this step using the skip button.
6. The wizard sends a GET request to the templating service with a query string of the format:
    ```
    ?template_location=databricksConfig.props.tt
    ```
7. The templating service will fail to satisfy the `lakefs_url` template property and will return **‘400 Bad Request: error code 1’** to the wizard.
8. The wizard will prompt a message saying that some needed information were not specified and that he should make sure he typed everything along the way.
9. Continue with the flow as described above…

### Sad flow - No fetching permissions

1. The user clicks on ‘Create Repository’, then clicks on ‘Spark Quickstart’
2. The wizard starts by showing an input textbox for the users to enter the repo name. The user types ‘spark-repo’
3. The wizard creates a repo named ‘spark-repo’ and sets a default ‘main’ branch.
4. The wizard asks the users if they want to import existing data to lakeFS. The user skips this step using the skip button.
6. The wizard sends a GET request to the templating service with a query string of the format:
    ```
    ?lakefs_url=<url>&template_location=databricksConfig.props.tt
    ```
7. The templating service tries to fetch the template from the provided location and will fail to do so as the server doesn’t have sufficient permissions.
8. The templating service will return **‘500 Internal Server Error’** to the wizard.
9. The wizard will prompt a message saying that the server could not access the requested template.
10. Continue with the flow as described above…

---

## Monitoring

### Operative Metrics

1. Templating service was called
    ```json
    {
        "class": "templating_service",
        "name": "calling_service",
        "value": "<service name>"
    }
    ```
2. Templating service- status 200
    ```json
    {
        "class": "templating_service",
        "name": "successful_call",
        "value": "<service name>"
    }
    ```
3. Templating service - status 500 - no access to provided template location
    ```json
    {
        "class": "templating_service",
        "name": "no_access",
        "value": "<service name>"
    }
    ```
4. Templating service - status 5xx - general
    ```json
    {
        "class": "templating_service",
        "name": "5xx",
        "value": "<service name>"
    }
    ```
5. Templating service - status 4xx - general
    ```json
    {
        "class": "templating_service",
        "name": "4xx",
        "value": "<service name>"
    }
    ```

### BI Metrics

Sent directly from the GUI Wizard

1. Wizard GUI - Quickstart started
    ```json
    {
      "class": "spark_wizard",
      "name": "quickstart_start",
      "value": 1
    }
    ```
2. Wizard GUI - Import data requested
    ```json
    {
      "class": "spark_wizard",
      "name": "import_data",
      "value": 1
    }
    ```
3. Wizard GUI - Spark config generated
    ```json
    {
      "class": "spark_wizard",
      "name": "generate_spark_template",
      "value": 1
    }
    ```
4. Wizard GUI - Quickstart ended
    ```json
    {
      "class": "spark_wizard",
      "name": "quickstart_end",
      "value": 1
    }
    ```

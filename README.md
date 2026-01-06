# Tovala Data Team Local Airflow Development (Using MWAA local Docker Images)
## Prerequisites
1. Check existing brew installed packages - `brew list` - must have the following (it's ok if you have additional packages, as long as you have these):
    - `Python@3.12`
    - `postgresql`
    - `rust`
    - `virtualenv`
2. For any packages missing, run `brew install ${package}` (e.g. brew install postgresql)
3. Confirm with `brew list` that all packages listed in Step 1 are present
4. Update your PATH to make sure you use Python 3.12 
    - Update your bash profile (`~/.zshrc`) with the following:
        ```
        export PATH="/opt/homebrew/bin/python3.12:/opt/homebrew/bin:$PATH"
        alias pip="pip3.12"
        alias python="python3.12"
        alias python3="python3.12"
        ```
    - **IMPORTANT** Order matters with path - so make sure `/opt/hoomebrew/bin/python3.12:` is the first item in the list
5. Restart terminal for changes to profile to take effect
6. `python --version` should now return `3.12.12`

## Quickstart
### Initial Setup Tasks
**Note: The items in this section only need to be done the first time you set up your local dev environment.**
1. Add the following environment variables to your bash profile (e.g. `./zshrc` on Mac)
    ```
    # Environment variables required to run  MWAA Local
    export MWAA_LOCAL_DEV="${add your name here}-local-dev" # e.g. "engineername-local-dev"
    export TOVALA_DATA_REGION="us-east-1"
    export TOVALA_DATA_AWS_ACCOUNT_ID=307553795271
    export TOVALA_DATA_AWS_KEY=${add your AWS access key for the data team account here}
    export TOVALA_DATA_AWS_SECRET=${add your AWS secret key for the data team account here}
    ```
    After saving, run `source ~/.zshrc` or restart Terminal for changes to take effect

2. Create venv
    - In the root directory of your local clone of this repo (`cd line_cook` in Terminal), run:
        
        ```
        python3 create_venvs.py --target development
        ```

### Using MWAA Local Development Environment
1. **Stand Up MWAA local**

    1a. Navigate to the `images/airflow/3.0.6/` directory:

        
        cd images/airflow/3.0.6
    1b. From within `images/airflow/3.0.6/`, run:
       
        ./run.sh    
  - MWAA local will build and run all the necessary containers and automatically create the following CloudWatch log groups:
      - `${MWAA_LOCAL_DEV}-DAGProcessing`
      - `${MWAA_LOCAL_DEV}-Scheduler`
      - `${MWAA_LOCAL_DEV}-Worker`
      - `${MWAA_LOCAL_DEV}-Task`
      - `${MWAA_LOCAL_DEV}-WebServer`
2. **Open Airflow UI**
    - In your web browser, navigate to [localhost:8080](http://localhost:8080)
3. **View Dags**
    - In the left-hand navigation bar, select `Dags`
    - Should see all current Tovala data team Dags from `images/airflow/3.0.6/dags`
4. **Local Testing**
    - This repo approximates the MWAA environment locally for testing
    - For local development and testing, there are 2 directories in the mwaa-local repo to focus on
        - `images/airflow/3.0.6/dags` - Folder containing all dags.
          - Will sync to prod S3 bucket on merge to `master` via github action to deploy dags
          - Any code in this directory in a PR **WILL** be deployed to Prod when merged, if code is here in Staging, it means it is ok to be deployed.

        - `images/airflow/3.0.6/requirements/requirements.txt` - Where to add airflow providers/python packages required to run dags.
          - Will sync to prod S3 bucket on merge to `master` (if changes are detected) via github action to deploy latest requirements.txt
          - When updated requirements are deployed, the MWAA Prod environment has to be manually updated (~30mins)
    
    - **Make sure to clean up Docker containers, images, and volumes when done testing!!**


# AWS Parent Repo README
## aws-mwaa-docker-images

## Overview

This repository contains the Docker Images that [Amazon
MWAA](https://aws.amazon.com/managed-workflows-for-apache-airflow/) uses to run Airflow.

You can also use it locally if you want to run a MWAA-like environment for testing, experimentation,
and development purposes.

Currently, Airflow v2.9.2 and above are supported. Future versions in parity with Amazon MWAA will be added as
well. _Notice, however, that we do not plan to support previous Airflow versions supported by MWAA._

## Using the Airflow Image

To experiment with the image using a vanilla Docker setup, follow these steps:

0. _(Prerequisites)_ Ensure you have:
   - Python 3.11 or later.
   - [Docker](https://docs.docker.com/desktop/) and [Docker Compose](https://docs.docker.com/compose/install/)
1. Clone this repository.
2. This repository makes use of Python virtual environments. To create them, from the root of the
   package, execute the following command:

```
python3 create_venvs.py --target <development | production>
```

3. Build a supported Airflow version Docker image
   - `cd <amazon-mwaa-docker-images path>/images/airflow/2.9.2`
   - Update `run.sh` file with your account ID, environment name and account credentials, api-server URL 
   - (`http://host_name:8080`). The permissions associated
   with the provided credentials will be assigned to the Airflow components that would be started with the next step. 
   So, if you receive any error message indicating lack of permissions, then try providing the permissions to the 
   identity whose credentials were used.
   - `./run.sh` This will build and run all the necessary containers and automatically create the following CloudWatch log groups:
     - `{ENV_NAME}-DAGProcessing`
     - `{ENV_NAME}-Scheduler`
     - `{ENV_NAME}-Worker`
     - `{ENV_NAME}-Task`
     - `{ENV_NAME}-WebServer`

Airflow should be up and running now. You can access the web server on your localhost on port 8080.

### Authentication from version 3.0.1 onward
For environments created using this repository starting with version 3.0.1, we default to using `SimpleAuthManager`, 
which is also the default auth manager in Airflow 3.0.0+. By default, `SIMPLE_AUTH_MANAGER_ALL_ADMINS` is set to true, 
which means no username/password is required, and all users will have admin access. You can specify users and roles 
using the SIMPLE_AUTH_MANAGER_USERS environment variable in the format:
```
username:role[,username2:role2,...]
```
To enforce authentication with explicit user passwords and roles, set:

```
SIMPLE_AUTH_MANAGER_ALL_ADMINS=false
```
In this mode, a password will be automatically generated for each user and printed in the webserver logs as soon as 
webserver starts.


### Generated Docker Images

When you build the Docker images of a certain Airflow version, using either `build.sh` or `run.sh`
(which automatically also calls `build.sh` for you), multiple Docker images will actually be
generated. For example, for Airflow 2.9, you will notice the following images:

| Repository                        | Tag                           |
| --------------------------------- | ----------------------------- |
| amazon-mwaa-docker-images/airflow | 2.9.2                         |
| amazon-mwaa-docker-images/airflow | 2.9.2-dev                     |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer                |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-dev            |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-privileged     |
| amazon-mwaa-docker-images/airflow | 2.9.2-explorer-privileged-dev |

Each of the postfixes added to the image tag represents a certain build type, as explained below:

- `explorer`: The 'explorer' build type is almost identical to the default build type except that it
  doesn't include an entrypoint, meaning that if you run this image locally, it will not actually
  start Airflow. This is useful for debugging purposes to run the image and look around its content
  without starting airflow. For example, you might want to explore the file system and see what is
  available where.
- `privileged`: Privileged images are the same as their non-privileged counterpart except that they
  run as the `root` user instead. This gives the user of this Docker image
  elevated permissions. This can be useful if the user wants to do some experiments as the root
  user, e.g. installing DNF packages, creating new folders outside the airflow user folder, among
  others.
- `dev`: These images have extra packages installed for debugging purposes. For example, typically
  you wouldn't want to install a text editor in a Docker image that you use for production. However,
  during debugging, you might want to open some files and inspect their contents, make some changes,
  etc. Thus, we install an editor in the dev images to aid with such use cases. Similarly, we
  install tools like `wget` to make it possible for the user to fetch web pages. For a complete
  listing of what is installed in `dev` images, see the `bootstrap-dev` folders.


## Extra commands

#### Requirements

- Add Python dependencies to `requirements/requirements.txt`
- To test a `requirements.txt` without running Apache Airflow, run:
```bash
./run.sh test-requirements
```

#### Startup script

- There is a folder in each airflow version called `startup_script`. Add your script there as `startup.sh`
- If there is a need to run additional setup (e.g. install system libraries, setting up environment variables), please modify the `startup.sh` script.
- To test a `startup.sh` without running Apache Airflow, run:
```bash
./run.sh  test-startup-script
```

#### Reset database

- If you encountered [the following error](https://issues.apache.org/jira/browse/AIRFLOW-3678): `process fails with "dag_stats_table already exists"`, you'll need to reset your database. You just need to restart your container by exiting and rerunning the `run.sh` script

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

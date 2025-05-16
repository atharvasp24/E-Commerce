# Project-2



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://git.rc.rit.edu/s25-dsci-644-shreyas/project-2.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](https://git.rc.rit.edu/s25-dsci-644-shreyas/project-2/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
The Candy Store Data Processing System is a Python-based pipeline designed to process candy store transaction data, generate summary tables, update inventory, and forecast sales and profits. It integrates data from MySQL (customers and products) and MongoDB (daily transactions from February 1-10, 2024), processes it using Apache Spark, and automates the workflow with Apache Airflow. This project is developed for the DSCI-644 course at RIT (Spring 2025).

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Requirements
- Operating System: Linux (tested on Ubuntu/WSL), macOS, or Windows with WSL
- Python Version: 3.12 (via Miniconda)

Dependencies:
    apache-airflow==2.7.0
    pyspark==3.5.0
    pymongo
    mysql-connector-python
    python-dotenv
    prophet
    pandas
    numpy
    scikit-learn

Steps:

1. Install Miniconda:
`wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
source ~/.bashrc`
2. Create Conda Environment:
`conda create -n candy_store python=3.12
conda activate candy_store`
3. Install Dependencies:
`pip install apache-airflow==2.7.0 pyspark==3.5.0 pymongo mysql-connector-python python-dotenv prophet pandas numpy scikit-learn`
4. Download MySQL Connector JAR:
- Download mysql-connector-java-8.0.33.jar from MySQL.
- Place it in a known directory.
5. Set Environment Variables:
Create a .env file in your project root:

`DATASET_NUMBER='16'  # Enter your dataset number (e.g., '1', '2', etc.)
MYSQL_CONNECTOR_PATH='/Users/atharvapatil/Downloads/RIT/SW_for_DS/mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar'  # Path to your MySQL connector JAR file
MONGODB_URI='mongodb://localhost:27017'
MYSQL_URL="jdbc:mysql://localhost:3306/candy_store_${DATASET_NUMBER}"
MYSQL_USER='athu24'  # Your MySQL username
MYSQL_PASSWORD='ayushi13'  # Your MySQL password
MYSQL_DB="candy_store_${DATASET_NUMBER}"
CUSTOMERS_TABLE='customers'
PRODUCTS_TABLE='products'
MONGO_DB="candy_store_${DATASET_NUMBER}"
MONGO_COLLECTION_PREFIX='transactions_'
MONGO_START_DATE=20240201 
MONGO_END_DATE=20240210
OUTPUT_PATH="data/output"`

6. Initialize Airflow:
`airflow db init`
## Usage
Running Locally (main.py)

1. Navigate to the project directory:
/Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2
2. Execute:
`python main.py`
3. Outputs are saved in /Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/output

**Running with Airflow (candy_store_dag.py)**

1. Copy Files to Airflow DAGs:
cp /Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/src/candy_store_dag.py \
   /Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/src/main.py \
   /Users/atharvapatil/Downloads/RIT/SW_for_DS/project-2/src/time_series.py \
   ~/airflow/dags/

2. Start Airflow:
`airflow webserver --port 8081 & airflow scheduler`

## Roadmap
- Implement CI/CD for automated testing.
- Add ARIMA forecasting alongside Prophet.
- Enable real-time transaction processing.


## Contributing
Contributions are welcome for DSCI-644 collaboration! To contribute:

1. Fork the repository.
2. Create a branch: git checkout -b feature/your-feature.
3. Commit changes: git commit -m "Add your feature".
4. Push: git push origin feature/your-feature.
5. Submit a merge request via Creating Merge Requests.

- Invite Members:
Go to Manage > Members.
Select Invite members, add usernames/emails, assign roles (e.g., Developer), and invite.

- Merge Requests:
From Web Editor: Edit a file, commit to a new branch, and check Create a merge request.
From Issues: Select Create merge request at the bottom of an issue.

- Issues:
Create: Go to Plan > Issues > New issue.
Bulk Edit: Select Bulk edit, update multiple issues, and save.



## Authors
Atharva Patil: Developer (RIT DSCI-644, Spring 2025).

## License
MIT License - see LICENSE for details.
## Project status
Active development for DSCI-644 Project-2. Post-submission, maintenance will be minimal unless extended for future coursework.
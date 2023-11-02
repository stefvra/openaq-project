# Openaq-project

The goal of this project is to ingest air quality data from an AWS sqs source and present it to the customer in a dashboard. It consists out of 3 parts:

- Data exploration
- Setup of AWS cloud services
- Dashboarding

## Data exploration

See [notebook](/notebooks/exploration.ipynb).

## Setup of AWS cloud services

The goal is to store the air quality data in a database. Therefore 2 resources are created:
- A DynamoDB: This database contains the measurements. A noSQL dabatase is choosen due to simplicity. The primary key is a combination of the parameter (pollutant) and the time of measurement. This time is stored as a timestamp numeric value. The resolution is microseconds so the chance of equal primary keys is nihil.
- Lambda function: this function is triggered by the sqs queue. It processes the measurements and ingests them into the database. Functionality is minimal.


## Dashboarding

The Data from the DynamoDB is provisioned in a user interface based on the [Dash](https://dash.plotly.com/) framework. The app has 3 components:
- Selector for selecting the pollutant. When selecting a pollutant, all other visuals are updated.
- A map that shows the selected pollutant as a collored circle. The value shown is the mean value of the last 6 hours.
- A graph that shows a longer history for a certain location. The default timeframe is 7 days. The location can be selected by clicking on the map.

![title](/assets/dash_screenshot.png)

## Installation

Below steps for installation with Visual Studio Code are listed:
- Clone this repository to your local maching
- Execute ```poetry install``` in the project home directory
- Install AWS Toolkit for Visual Studio Code [link](https://aws.amazon.com/visualstudiocode/)
- Add a profile to AWS Toolkit ([instructions](https://docs.aws.amazon.com/toolkit-for-vscode/latest/userguide/connect.html)). If not using VSC, profile credentials need to be given in a different manner, eg in the code.
- Add _secrets.py file, see [example](/config/_secrets_example.py)

To run dash app: 
poetry 



## Possible improvements

- Add testing to the package code
- Add git precommit hooks for code quelity checking (eg black...)
- Split repository in smalles parts (eg Dash app, exploration part, packaged modules...)
- Provision the Dash app on a web service
- Add data quality monitoring to the streaming services
- Make lambda function more robust (eg message variations)

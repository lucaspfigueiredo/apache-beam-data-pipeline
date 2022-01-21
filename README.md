# Apache Beam Data Pipeline
[![Python 3.8](https://img.shields.io/badge/python-3.8-blue.svg)](https://www.python.org/downloads/release/python-360/)

![Data Pipeline](/images/image.png)


### Introduction

The analytics team has decided to study if the cases of dengue is correlated with the increase in precipitation during the last years. To achieve this goal, we are going to use one dataset of the rainfall volume and another dataset of dengue cases throughout the Brazilian states. 

In this project, developed during the [Alura]("https://www.alura.com.br/") class, we created a data pipeline using Apache Beam, applying transform methods and merging the two datasets persisting the data in CSV format. 


### Getting started

#### Requirements

* Install [Python3](https://www.python.org/downloads/)


#### Clone repository to local machine
```
git clone https://github.com/lucaspfigueiredo/apache-beam-data-pipeline.git
```

#### Change directory to local repository
```
cd apache-beam-data-pipeline
```

#### Create python virtual environment
```
python3 -m venv .venv             # create virtualenv
source .venv/bin/activate         # activate virtualenv
pip install -r requirements.txt   # install requirements
``` 

### References

* [Alura Apache Beam]("https://www.alura.com.br/curso-online-apache-beam-data-pipeline-python")

* [Apache Beam]("https://beam.apache.org/documentation/")


### License
Distributed under the MIT License. See [LICENSE](LICENSE) for more information.
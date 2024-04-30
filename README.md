# ACCOUNT ENTRIES PROCESSOR

This project process account entries subtracting withdraw amount from balance based on CSV input files containing withdraws and balances. It uses Pyspark to handle and process data. 

# 📖 Table of Contents

- [🚀 About The Project](#about-the-project)
  * [Balances](#balances)
  * [Withdraws](#withdraws)
  * [Rules](#rules)
  * [Result](#result)
- [🛠️ Getting Started](#getting-started)
  * [Build with](#build-with)
  * [Requirements](#requirements)
    + [☕ JDK](#jdk)
    + [✴️ Spark](#spark)
    + [🐍 Python](#python)
      - [venv](#venv)
- [⚡ Running The Project](#running-the-project)
- [💡 Improvements](#improvements)

# About The Project

There are two kinds of inputs:

## Balances
Represents the amount available for withdraws operations. It has the following data:
- account_id;
- balance_order;
- available_balance; 
- status.

## Withdraws
Represents the amount desired to be withdrew from account available balance. It is composed by:
- account_id;
- withdraw_amount;
- withdraws_order.

## Rules
The withdraw amount of a given withdraw order has to be subtracted from the balance (from one or more balance entries), always considering balance_order in ascending order.

- If a balance value is not enough, check if the account_id has more than one balance at its disposal and, if so, withdraw the rest from another balance in the same account_id;
- If the withdraw value is greater than the sum of all balance values left in the account, the withdraw should not happen;
- If a balance in the account reaches 0, it's status should be changed to "BALANCE WITHDREW";
0 The initial_balance has to have the initial value before the withdraws, and the available_balance has to reflect how much was left after the withdraws;
- validation_result shows if the withdraw happened or not.

## Result
Account entries after processed will be presented as:
- account_id;
- balance_order_id;
- initial_balance;
- available_balance
- status;
- validation_result.

# Getting Started

## Build with

This project is based on:
- 🐍 Python 3.9 
- ✴️ Spark 3.5.1
- ☕ JDK 11.0.22

## Requirements
For Linux based systems:
### JDK
To check if JDK is installed, run: `java --version `
The output should look like this:
```
openjdk 11.0.22 2024-01-16
OpenJDK Runtime Environment (build 11.0.22+7-post-Ubuntu-0ubuntu222.04.1)
OpenJDK 64-Bit Server VM (build 11.0.22+7-post-Ubuntu-0ubuntu222.04.1, mixed mode, sharing)
``` 

If it is not installed, you can install it with these commands:
```
sudo apt update
sudo apt install default-jdk
```
### Spark
To check if Spark is installed, run: `spark-submit --version`
 The output should look like this:
```
 Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.1
      /_/
            
Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 11.0.22
Branch HEAD
Compiled by user heartsavior on 2024-02-15T11:24:58Z
```
 
 If it is not installed, you can install it with these commands:
```
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
```

Then move the extracted folder to `/opt` directory:
```
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark
```
Then add these lines to `~/.bashrc` to set up the required environment variables:
```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

Reload `~/.bashrc` file to apply the changes:
```
source ~/.bashrc
```
### Python
I recommend using [pyenv](https://github.com/pyenv/pyenv ) to manage Python versions. If you're using it, run this command to install Python 3.9:
```
pyenv install 3.9
```
Run `pyenv versions` to list installed versions, confirm version 3.9 is listed.
Then in the root folder of this repo, run: `pyenv local 3.9`. 

#### venv
Create a virtual environment ([venv](https://docs.python.org/3/library/venv.html)) to isolate this project packages: 
```
python -m venv venv
```
To activate it, run:
```
source venv/bin/activate
```
Then install required packages:
```
pip install -r requirements.txt
```

# Running The Project
With venv activated and required packages already installed, run:
```
python src/main.py
``` 

Based on default input files for balances and withdraws found in `src/jobs/account_entries/resources/`, it will create a CSV output file containing the processed account balance entries after successful and unsuccessful withdraws in this path: `src/jobs/account_entries/result/`. 

Alternatively, you may provide your own input CSV files for balance and/or withdraws.  Use parameters `-b`/`--balances`  and `-w`/`--withdraws`, for example:
```
python src/main.py -b -w path/to/my/balances_file.csv -w path/to/my/withdraws_file.csv
python src/main.py --balances -w path/to/my/balances_file.csv --withdraws path/to/my/withdraws_file.csv
``` 

# Improvements
- Use pyspark entirely to handle account withdraw logic;
- Create tests for each of the job's functions;
- Use parallelization for better performance (RDDs);
- Create an interface to support multiple inputs like JSON, XML, databases, etc.

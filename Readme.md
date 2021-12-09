# Spark Installation Steps


## Java

- Download and install - Java 8 - [ Link ](https://www.oracle.com/in/java/technologies/javase/javase8u211-later-archive-downloads.html)
- Set Environment Variables 

  | Variable Name | Value                         |
  |---------------|-------------------------------|
  | JAVA_HOME     | C:\Progra~1\Java\jdk1.8.0_301 |
  | PATH          | %JAVA_HOME%\bin               |

  Note - <br>
  avoid using spaces in Environment Variables to avoid further issues <br> `
  Progra~1` is short for *Program Files* whereas `Progra~2` is short for *Program Files (x86)*
- Enter command to check java installation - `java --version`
- Output

  ```
  java version "1.8.0_301"
  Java(TM) SE Runtime Environment (build 1.8.0_301-b09)
  Java HotSpot(TM) 64-Bit Server VM (build 25.301-b09, mixed mode)  
  ```

## Python

- Download and install - Python 3.6+ - [ Link  - Python 3.8.10 ](https://www.python.org/ftp/python/3.8.10/python-3.8.10-amd64.exe)
- Ensure to check *Add Python to Path* option while installation
- verify if python is installed correctly - `python`
- Output - should start python console
  ```
  Python 3.8.10 (tags/v3.8.10:3d8993a, May  3 2021, 11:48:03) [MSC v.1928 64 bit (AMD64)] on win32
  Type "help", "copyright", "credits" or "license" for more information.
  >>> quit()
  ```


## Spark Installation

- Download Pre-Built Apache Spark with Hadoop [ Link ](https://dlcdn.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz)
- Extract the downloaded .tgz file to `C:\Binaries\spark-3.1.2-bin-hadoop3.2` - create folder if it doesn't exist
- Set Environment Variables

  | Variable Name | Value                                 |
  |---------------|---------------------------------------|
  | SPARK_HOME    | C:\Binaries\spark-3.1.2-bin-hadoop3.2 |
  | PATH          | %SPARK_HOME%\bin                      |
  | _JAVA_OPTIONS | -Xmx512M -Xms512M                     |

## Windows specific steps

- Download winutils - [ Link ](https://github.com/cdarlint/winutils/archive/refs/heads/master.zip)
- Copy files from directory hadoop-3.2.0 to `C:\Binaries\hadoop-3.2.0\bin` - create folder if it doesn't exist

  | Variable Name | Value                    |
  |---------------|--------------------------|
  | HADOOP_HOME   | C:\Binaries\hadoop-3.2.0 |
  | PATH          | %HADOOP_HOME%\bin        |
- Hive Permissions ( to be executed after winutils is added to PATH )
  - Create the folder `C:\tmp\hive`
  - Execute the following command in Admin Command Prompt <br> `winutils.exe chmod -R 777 C:\tmp\hive`
  - Check the permissions - `winutils.exe ls -F D:\tmp\hive`

## PySpark Setup

- For installing pyspark there are two ways
  1. using pip - `pip install pyspark==3.1.2`
  2. using source code
     1. Open `C:\Binaries\spark-3.1.2-bin-hadoop3.2\python` in command prompt
     2. Execute command - `python setup.py install`
- Set Environment Variables

  | Variable Name         | Value  |
  |-----------------------|--------|
  | PYSPARK_DRIVER_PYTHON | python |
  | PYSPARK_PYTHON        | python |

- To test pyspark installation, execute command - `pyspark`
- This should start pyspark interactive shell - sample output
  ```
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Python 3.8.10 (tags/v3.8.10:3d8993a, May  3 2021, 11:48:03) [MSC v.1928 64 bit (AMD64)] on win32
  Type "help", "copyright", "credits" or "license" for more information.
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
  Setting default log level to "WARN".
  To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
  21/12/09 11:22:43 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
  Welcome to
        ____              __
       / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
     /__ / .__/\_,_/_/ /_/\_\   version 3.1.2
        /_/
  
  Using Python version 3.8.10 (tags/v3.8.10:3d8993a, May  3 2021 11:48:03)
  Spark context Web UI available at http://AmeysPC:4041
  Spark context available as 'sc' (master = local[*], app id = local-1639029163317).
  SparkSession available as 'spark'.
  >>> 
  ```
  
## Jupyter Notebook 

- Install jupyter notebook using pip - `pip install jupyter`
- Launch jupyter notebook - `jupyter notebook`
- Execute the following code blocks in jupyter notebook

  ```python
  from pyspark import SparkContext, SparkConf
  from pyspark.sql import SparkSession
  ```
  ```python
  conf = SparkConf().setAppName('appName').setMaster('local')
  sc = SparkContext(conf=conf)
  spark = SparkSession(sc)
  ```
  ```python
  nums = sc.parallelize([1,2,3,4])
  nums.map(lambda x: x*x).collect()
  ```

## Scala Setup ( Optional )

- Scala is required to use spark-shell
- Download and install Scala - [ Link ](https://downloads.lightbend.com/scala/2.13.7/scala-2.13.7.msi)

  | Variable Name | Value             |
  |-------------------|------------------------------|
  | SCALA_HOME    | C:\Progra~2\scala |
  | PATH          | %SCALA_HOME%\bin  |

- Check if scala is working - `scala -version`
  ```
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Scala code runner version 2.13.7 -- Copyright 2002-2021, LAMP/EPFL and Lightbend, Inc.
  ```
- check if spark-shell is working - `spark-shell`
- Sample Output
  ```
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Picked up _JAVA_OPTIONS: -Xmx512M -Xms512M
  Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
  Setting default log level to "WARN".
  To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
  Spark context Web UI available at http://AmeysPC:4040
  Spark context available as 'sc' (master = local[*], app id = local-1639033396349).
  Spark session available as 'spark'.
  Welcome to
        ____              __
       / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
     /___/ .__/\_,_/_/ /_/\_\   version 3.1.2
        /_/
  
  Using Scala version 2.12.10 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_301)
  Type in expressions to have them evaluated.
  Type :help for more information.
  
  scala> 
  ```
  
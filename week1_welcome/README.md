# Hours with Experts - Week 1: Environment Setup

To set up your environment, we are going to install:

* Python 3.10
* Java 1.8
* Git Bash
* WinUtils
* Visual Studio Code ("VS Code")

Then set up:

* Environment variables relating to Java/Python/Spark
* A Python virtual environment
* VS Code

And finally execute:

* A  simple Spark program to make sure your environment on your computer is set up for Spark.

Follow the setup instructions specific to your OS.

Note: It is possible you already have other/more recent versions of Python and Java already on your computer (Java 1.8 came out in 2014!). However, it is very important to use exactly these versions: lots of data engineering projects were built off of Java 1.8, and these versions need to be compatible with the versions of the AWS libraries we will be using. You will encounter lots of errors - some obvious, some tricky ("UnsatisifedLinkError"? "ClassDefNotFound?") if your versions of Java, Python, and AWS libraries are not correctly in sync!

Other tools we will use (VS Code, Git) are not as sensitive, and if you already have these on your computer, whatever versions you have are likely fine.

### How to determine package versions

If you want the gory details of why we must run with:

pyspark 3.1.3
winutils 3.2.0
aws-java-sdk-bundle 1.11.375

Here is the link that explains:

https://github.com/chriscugliotta/pyspark-s3-windows
# CS4225-Data-System
CS4225 big data system for data science

### Setting up IDE on windows
* Download binary file from Hadoop and extract into 
```
C:\Program Files\
```
* Set the Environment variables:
<br/> Add to user variable
<br/> `HADOOP_BIN_PATH` : `%HADOOP_HOME%\bin`
<br/> `HADOOP_HOME` : `E:\soft\hadoop-2.7.4` (your Hadoop’s installation path)
<br/> `JAVA_HOME`: `C:\Program Files\Java\jdk-11.0.5`
<br/> Add to system variable Path
<br/> `%HADOOP_HOME%\bin` and `%HADOOP_HOME%\sbin`

* Download the files necessary for Windows(such as winutils.exe, hadoop.dll) from [this link](https://github.com/cdarlint/winutils)
<br/> find the version of your hadoop and put the files into `C:\Program Files\hadoop-2.9.2\bin`

* Setting up intellij
Please follow the part in [doc](https://github.com/CoderStellaJ/CS4225-Data-System/blob/master/setup_IDE_win_macos_linux.docx)

### Building cluster with docker
* Install Docker Toolbox or Docker Desktop
* Docker pull image
<br/> Open Docker Quickstart Terminal
<br/> `docker pull nusbigdatacs4225/ubuntu-with-hadoop-spark`
* Docker configure nodes
<br/> `docker run -it -h master --name master nusbigdatacs4225/ubuntu-with-hadoop-spark`
<br/> `ctrl+p` then `ctrl+q`

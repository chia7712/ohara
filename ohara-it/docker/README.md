### How to build a docker image of HDFS?

* Please follow below command to build namenode and data docker image:

```
# docker build -f namenode.dockerfile -t ghcr.io/chia7712/ohara/hdfs-namenode .
# docker build -f datanode.dockerfile -t ghcr.io/chia7712/ohara/hdfs-datanode .
```

### How to running the namenode docker container?
* You must set all datanode IP and hostname mapping to the /etc/hosts file or you have the
DNS Server to resolve datanode hostname

* You must confirm namenode port firewalld is close

* Please follow below command to run namenode container:

```
# docker run --net host -it ghcr.io/chia7712/ohara/hdfs-namenode
```

### How to running the datanode docker container?
* You must set namenode IP and hostname mapping to the /etc/hosts file or you have the
DNS Server toresolve namenode hostname

* You must confirm datanode port firewalld is close

* Please follow below command to run datanode container:

```
# docker run -it --env HADOOP_NAMENODE=${NAMENODE_HOST_NAME}:9000 --net host ghcr.io/chia7712/ohara/hdfs-datanode
```

### How to use the shell script to run the HDFS container?

```
$ bash hdfs-container.sh [start-all|start-namenode|start-datanode|stop-all|stop-namenode|stop-datanode] arg1 ...
```

### How to use the shell script to run the Oracle database container?

```
$ bash oracle-container.sh start --user ${USERNAME} --password ${PASSWORD}
```

### How to build the ftp server docker image?

```
$ docker build -f ftp.dockerfile -t ghcr.io/chia7712/ohara/ftp .
```
### How to run the fpt server docker container?

```
$ docker run -d --env FTP_USER_NAME=${FTP_LOGIN_USER_NAME} --env FTP_USER_PASS=${FTP_LOGIN_PASSWORD} --env FORCE_PASSIVE_IP=${YOUR_HOST_IP} -p 21:21 -p 30000-30009:30000-30009 ghcr.io/chia7712/ohara/ftp
```

### How to build the samba server docker image?

```
$ docker build -f samba.dockerfile -t ghcr.io/chia7712/ohara/samba .
```

### How to run the samba server docker container?

```
$ docker run -d --env SAMBA_USER_NAME=${user} --env SAMBA_USER_PASS=${password} -p 139:139 -p 445:445 ghcr.io/chia7712/ohara/samba
```

### How to use the start-service.sh script to start the hdfs, ftp, samba and oracle service?

```
$ bash start-service.sh [service name] [argument.....]
```

Below is support the service:

```
hdfs: Apache hadoop 2.7.0
ftp:  Apache ftpserver 1.1.1
samba: Samba 4.9.1
oracle: Oracle enterprise 12.2.0.1
```

Below command is example to start the samba service:

```
$ bash start-service.sh samba --user ohara --password password --sport 139 --dport 445 --host host1
```

### How to use the stop-service.sh script to stop the service?

```
$ bash stop-service.sh [service name] [argument.....]
```

Below command is example to stop the samba service:

```
$ bash stop-service.sh samba --host host1
```

### How to use the --help argument for the start-service.sh and stop-service.sh script?

Below command to show all service name

```
$ bash start-service.sh --help
```

Below command to show service argument
```
$ bash start-service.sh [service name] --help
```

example:
```
$ bash start-service.sh samba --help
```
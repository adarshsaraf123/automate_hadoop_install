# automate_hadoop_install
Hadoop installation is among the most difficult challenges people face. Handling the syncing of the Hadoop configurations across multiple nodes can be quite a headache. This tool makes use of the Python Fabric library to automate the installation of Hadoop over a cluster.

Although HBase installation is also supported it is yet to be fully functional and we currently provide only hadoop installation details.

# Prerequisites
1. This tool does not provide options to configure Hadoop but automate the distribution and running of Hadoop over a given list of master and slaves nodes. We require that all the Hadoop configuration, e.g. in _hdfs-site.xml_ or _core_site.xml_, be made on the master node first.
2. Provide the list of master and slave nodes in _global\_conf.py_ file. (note: we currently support only a single master).
3. In _hadoop\_fab.py_
   1. Provide the path for the hadoop directory in the global variable `hadoop_path`.
   2. Provide the path for the hadoop data directory in the global variable `hadoop_datadir_path`.

# Sample Commands
To install hadoop, once the above prerequisites have been made, run
```
fab install_hadoop
```

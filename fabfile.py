# Writen by Adarsh Saraf
# II M.Tech, SSSIHL, Jan 2017


from fabric.api import *
from fabric.context_managers import cd
from fabric.operations import put, prompt
from fabric.contrib.files import exists, append, sed
from fabric.tasks import execute

slaves = ['10.0.3.101',
          #'10.0.3.106',
          #'10.0.3.103',
          #'10.0.3.104',
          #'10.0.3.108',
        ]
master = '10.0.3.102'

hadoop_path = '~/Documents/trials/hadoop'
hadoop_tmp_path = '~/Documents/trials/hadoop_tmp'

hbase_path = '~/Documents/trials/hadoop'
hbase_zookeeper_path = '~/Documents/trials/zookeeper'

env.roledefs['slaves'] = slaves
env.roledefs['all'] = slaves + [master]

env.user = 'hduser'
env.shell = '/bin/bash -l -c -i' # to enable interactive runs of the commands in fabric; this ensures
                                # that the environment changes in bashrc are available during the fabric run

@roles('slaves')
def sync_date():
	run('sudo date --set="$(ssh hduser@{} date)"'.format(master))

@roles('slaves')
def sync_hadoop():
	run("rsync -avxP --exclude 'logs' {user}@{mn}:{hp}/ {hp}".format(user=env.user,mn=master, hp=hadoop_path))

@roles('all')
def export_hadoop_env_variables():
    # append the hadoop environment variables to the .bashrc file in all the nodes in the cluster
    hadoop_env = ["# -- HADOOP ENVIRONMENT VARIABLES START -- #",
                  "export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_65",
                  "export HADOOP_HOME={}".format(hadoop_path), 
                  #"export YCSB_HOME=/home/hduser/Documents/ycsb",
                  "export PATH=$PATH:$HADOOP_HOME/bin",
                  "export PATH=$PATH:$HADOOP_HOME/sbin",
                  #"export PATH=$PATH:$YCSB_HOME/bin",
                  "export HADOOP_MAPRED_HOME=$HADOOP_HOME",
                  "export HADOOP_COMMON_HOME=$HADOOP_HOME",
                  "export HADOOP_HDFS_HOME=$HADOOP_HOME",
                  "export YARN_HOME=$HADOOP_HOME",
                  "# -- HADOOP ENVIRONMENT VARIABLES END -- #",
                ]
    append('~/.bashrc', hadoop_env)
    
    
@roles('all')
def export_hbase_env_variables():
    '''
    Append the hbase environment variables to the .bashrc file in all the nodes in the cluster
    '''
    hbase_env = [ "export HBASE_HOME=/home/hduser/Documents/hbase",
                  "export PATH=$PATH:$HBASE_HOME/bin",
                ]
    append('~/.bashrc', hadoop_env)
    
@roles('all')
def reset_hadoop_tmp():
    # now we need to create the hadoop data dirs
    if exists(hadoop_tmp_path):
        if 'reset_hadoop' not in env:
            p = prompt('are you sure you want to reset the existing hadoop data dirs? (y/n)', key='reset_hadoop', default='y')
        if env.reset_hadoop == 'y':
            run('rm -R {}'.format(hadoop_tmp_path))
            run('mkdir -p {}'.format(hadoop_tmp_path))

@hosts(master)
def setup_hadoop_master():
    '''
    This task has to be executed only on the master node of the cluster. 
    '''
    # prepare the masters file locally 
    fmaster = open('masters', 'w')
    fmaster.write(master)
    fmaster.close()
    
    # prepare the slaves file locally
    fslaves = open('slaves', 'w')
    fslaves.write(master + '\n')
    for s in slaves:
        fslaves.write(s + '\n')
    fslaves.close()
    
    # now put the masters and slaves file to the master
    put('masters', hadoop_path + '/etc/hadoop')
    put('slaves', hadoop_path + '/etc/hadoop')
    for s in slaves:
        run('ssh-copy-id -i ~/.ssh/id_rsa.pub {}@{}'.format(env.user, s))
    
    export_hadoop_env_variables()
    reset_hadoop_tmp()
    
@hosts(master)
def setup_hbase_master():
    '''
    This task has to be executed only on the master node of the cluster. 
    '''
    # prepare the regionservers file locally 
    file_regionserver = 'regionservers'
    fregionservers = open(file_regionserver, 'w')
    for s in env.roledefs['all']:
        fregionservers.write(s + '\n')
    fregionservers.close()
    
    # now put the masters and slaves file to the master
    put(file_regionserver, hbase_path + '/conf')
    export_hbase_env_variables()


@roles('slaves')
def setup_hadoop_slaves():
    '''
    This task has to be executed on all the nodes in the cluster.
    '''
    # create the hadoop_tmp directory used for holding the namenode and datanode folders as set in hadoop conf files
    run('ssh-copy-id -i ~/.ssh/id_rsa.pub {}@{}'.format(env.user, master))
    if not exists(hadoop_path):
        run('mkdir -p {}'.format(hadoop_path))
    sync_hadoop()
    reset_hadoop_tmp()
    
    export_hadoop_env_variables()

@roles('slaves')
def setup_hbase_slaves():
    '''
    This task has to be executed on all the nodes in the cluster.
    '''
    # create the hadoop_tmp directory used for holding the namenode and datanode folders as set in hadoop conf files
    if not exists(hbase_path):
        run('mkdir -p {}'.format(hbase_path))
    sync_hbase()
    
    export_hbase_env_variables()

@hosts(master)
def namenode_format():
    run('hdfs namenode -format')

def hadoop_install():
    execute(setup_hadoop_master)
    execute(setup_hadoop_slaves)
    execute(namenode_format) 

@hosts(master)
def reset_namenode():
    execute(reset_hadoop_tmp)
    namenode_format()

@hosts(master)
def start_hadoop():
	run('start-dfs.sh')
	run('start-yarn.sh')
	#run('start-hbase.sh')

@hosts(master)
def stop_hadoop():
	#run('stop-hbase.sh')
	run('stop-yarn.sh')
	run('stop-dfs.sh')

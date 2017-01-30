# Writen by Adarsh Saraf
# II M.Tech, SSSIHL, Jan 2017

from global_conf import *

hbase_path = '~/Documents/trials/hadoop'
hbase_zookeeper_path = '~/Documents/trials/zookeeper'

@roles('all')
def export_hbase_env_variables():
    '''
    Append the hbase environment variables to the .bashrc file in all the nodes in the cluster
    '''
    hbase_env = [ "export HBASE_HOME=/home/hduser/Documents/hbase",
                  "export PATH=$PATH:$HBASE_HOME/bin",
                ]
    append('~/.bashrc', hadoop_env)

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
def install_hbase():
    '''
    Task to install hbase on the clusters
    '''
    sync_date()
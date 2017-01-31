# Writen by Adarsh Saraf
# II M.Tech, SSSIHL, Jan 2017

from global_conf import *

hbase_path = '~/Documents/trials/hbase'
hbase_zookeeper_path = '~/Documents/trials/zookeeper'

#---------------------------------------------------------------------------------------------------------------
#--------------------------------------OPERATIONS ON ALL THE HBASE NODES---------------------------------------
#---------------------------------------------------------------------------------------------------------------

@roles('all')
def export_hbase_env_variables():
    '''
    Append the hbase environment variables to the .bashrc file in all the nodes in the cluster
    '''
    hbase_env = [ "export HBASE_HOME=/home/hduser/Documents/hbase",
                  "export PATH=$PATH:$HBASE_HOME/bin",
                ]
    append('~/.bashrc', hbase_env)

@roles('all')
def reset_hbase_zookeeper():
    '''
    To reset the HBase zookeeper directory in preparation for a fresh HBase install
    '''
    # now we need to create the hadoop data dirs
    if exists(hbase_zookeeper_path):
        if 'reset_hbase' not in env:
            prompt('are you sure you want to reset the existing hadoop data directory? (y/n)',
                   key='reset_hbase', default='y')
        if env.reset_hbase == 'y':
            run('rm -R {}'.format(hbase_zookeeper_path))
            run('mkdir -p {}'.format(hbase_zookeeper_path))
        
#---------------------------------------------------------------------------------------------------------------
#---------------------------------------OPERATIONS ON THE MASTER NODE-------------------------------------------
#---------------------------------------------------------------------------------------------------------------

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
    put(file_regionserver, hbase_path + '/conf/')
    export_hbase_env_variables()
    reset_hbase_zookeeper()
    
#---------------------------------------------------------------------------------------------------------------
#---------------------------------------OPERATIONS ON THE SLAVE NODES-------------------------------------------
#---------------------------------------------------------------------------------------------------------------

@roles('slaves')
def sync_hbase():
    '''
    To sync the hadoop folder, with its configurations, at the master with the slaves 
    '''
    run("rsync -avxP --exclude 'logs' {user}@{mn}:{hp}/ {hp}".format(
                                        user=env.user,mn=master, hp=hbase_path))

@roles('slaves')
def setup_hbase_slaves():
    '''
    This task has to be executed on all the nodes in the cluster.
    '''
    # create the hbase directory used for holding the namenode and datanode folders as set in hadoop conf files
    if not exists(hbase_path):
        run('mkdir -p {}'.format(hbase_path))
    
    export_hbase_env_variables()
    sync_hbase()
    reset_hbase_zookeeper()
    

#---------------------------------------------------------------------------------------------------------------
#-------------------------------ADMINISTRATION TASKS FOR THE HADOOP CLUSTER-------------------------------------
#---------------------------------------------------------------------------------------------------------------

@hosts(master)
def install_hbase():
    '''
    To install hbase on the cluster on the nodes as specified in global_conf
    '''
    execute(sync_date)
    execute(setup_hbase_master)
    execute(setup_hbase_slaves)

@hosts(master)
def start_hbase():
    '''
    To start hbase alongwith the hadoop cluster
    '''
    run('start-dfs.sh')
    run('start-yarn.sh')
    run('start-hbase.sh')

@hosts(master)
def stop_hbase():
    '''
    To stop hbase alongwith the hadoop cluster
    '''
    run('stop-hbase.sh')
    run('stop-yarn.sh')
    run('stop-dfs.sh')
    

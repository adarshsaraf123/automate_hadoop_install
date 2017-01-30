# Writen by Adarsh Saraf
# II M.Tech, SSSIHL, Jan 2017


from global_conf import *

hadoop_path = '~/Documents/trials/hadoop'
hadoop_datadir_path = '~/Documents/trials/hadoop_datadir'

#---------------------------------------------------------------------------------------------------------------
#--------------------------------------OPERATIONS ON ALL THE HADOOP NODES---------------------------------------
#---------------------------------------------------------------------------------------------------------------

@roles('all')
def export_hadoop_env_variables():
    '''
    To append the hadoop environment variables to the .bashrc file in all the nodes in the cluster
    '''
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
def reset_hadoop_datadir():
    '''
    To reset the Hadoop data directory in preparation for a fresh namenode format
    '''
    # now we need to create the hadoop data dirs
    if exists(hadoop_datadir_path):
        if 'reset_hadoop' not in env:
            prompt('are you sure you want to reset the existing hadoop data directory? (y/n)', key='reset_hadoop', default='y')
        if env.reset_hadoop == 'y':
            run('rm -R {}'.format(hadoop_datadir_path))
            run('mkdir -p {}'.format(hadoop_datadir_path))

#---------------------------------------------------------------------------------------------------------------
#---------------------------------------OPERATIONS ON THE MASTER NODE-------------------------------------------
#---------------------------------------------------------------------------------------------------------------

@hosts(master)
def setup_hadoop_master():
    '''
    To setup the master node for Hadoop 
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
    
    # enable password-less login for the master on all the slaves
    for s in slaves:
        run('ssh-copy-id -i ~/.ssh/id_rsa.pub {}@{}'.format(env.user, s))
    
    # export the environment variables necessary for running the hadoop in the bashrc file
    export_hadoop_env_variables()

@hosts(master)
def namenode_format():
    '''
    To format the namenode
    '''
    run('hdfs namenode -format')

#---------------------------------------------------------------------------------------------------------------
#---------------------------------------OPERATIONS ON THE SLAVE NODES-------------------------------------------
#---------------------------------------------------------------------------------------------------------------


@roles('slaves')
def sync_hadoop():
    '''
    To sync the hadoop folder, with its configurations, at the master with the slaves 
    '''
    run("rsync -avxP --exclude 'logs' {user}@{mn}:{hp}/ {hp}".format(user=env.user,mn=master, hp=hadoop_path))

@roles('slaves')
def setup_hadoop_slaves():
    '''
    To setup the slave ndoes for Hadoop
    '''
    # enable password-less login from the slaves to the master
    run('ssh-copy-id -i ~/.ssh/id_rsa.pub {}@{}'.format(env.user, master))
    
    # create the hadoop directory
    if not exists(hadoop_path):
        run('mkdir -p {}'.format(hadoop_path))
    
    # sync the hadoop directory from the master
    sync_hadoop()
    
    # export the environment variables necessary for running the hadoop in the bashrc file
    export_hadoop_env_variables()


#---------------------------------------------------------------------------------------------------------------
#-------------------------------ADMINISTRATION TASKS FOR THE HADOOP CLUSTER-------------------------------------
#---------------------------------------------------------------------------------------------------------------


def reset_namenode():
    '''
    To reset the namenode, and thereby reset the HDFS data
    '''
    execute(reset_hadoop_datadir)
    execute(namenode_format)

def install_hadoop():
    '''
    To install the hadoop cluster on the nodes as specified in global_conf
    '''
    execute(setup_hadoop_master)
    execute(setup_hadoop_slaves)
    reset_namenode()

@hosts(master)
def start_hadoop():
    '''
    To start the hadoop cluster
    '''
    run('start-dfs.sh')
    run('start-yarn.sh')
    #run('start-hbase.sh')

@hosts(master)
def stop_hadoop():
    '''
    To stop the hadoop cluster
    '''
    #run('stop-hbase.sh')
    run('stop-yarn.sh')
    run('stop-dfs.sh')


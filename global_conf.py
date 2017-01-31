# Writen by Adarsh Saraf
# II M.Tech, SSSIHL, Jan 2017


from fabric.api import env, roles, run, hosts
from fabric.operations import put, prompt
from fabric.contrib.files import exists, append
from fabric.tasks import execute

slaves = ['10.0.3.101',
          '10.0.3.106',
          '10.0.3.103',
          '10.0.3.104',
          '10.0.3.108',
        ]

master = '10.0.3.102'

env.roledefs['slaves'] = slaves
env.roledefs['all'] = slaves + [master]

env.user = 'hduser'
env.shell = '/bin/bash -l -c -i' # to enable interactive runs of the commands in fabric; this ensures
                                # that the environment changes in bashrc are available during the fabric run
                                
@roles('slaves')
def sync_date():
    '''
    Sync the system dates across all the nodes in the cluster
    '''
    run('sudo date --set="$(ssh {}@{} date)"'.format(env.user, master))
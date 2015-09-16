node default {

  package { [
    'software-properties-common',
    'heimdal-clients'
    ]: ensure => 'installed',
  }

  apt::source { 
    'cloudera':
      location => 'http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh',
      release => 'trusty-cdh5',
      repos    => 'contrib',
      architecture => 'amd64',
      ensure => 'present',
      include  => {
        'src' => true,
        'deb' => true,
      },
    ;
    'cloudera-manager':
      location => 'http://archive.cloudera.com/cm5/ubuntu/trusty/amd64/cm',
      release => 'trusty-cm5',
      repos    => 'contrib',
      architecture => 'amd64',
      ensure => 'present',
      include  => {
        'src' => true,
        'deb' => true,
      },
  }

  include stdlib
  include apt
  include java8

  class {
    'hadoop':
      hdfs_hostname => $::fqdn,
      yarn_hostname => $::fqdn,
      slaves => [ $::fqdn ],
      frontends => [ $::fqdn ],
      realm => '',
      properties => {
        'dfs.replication' => 1,
      }
    ;
    'spark':
      hdfs_hostname => $::fqdn,
    ;
 }
  
  include hadoop::namenode
  include hadoop::resourcemanager
  include hadoop::historyserver
  include hadoop::datanode
  include hadoop::nodemanager
  include hadoop::frontend

  include spark::frontend
  # should be collocated with hadoop::namenode
  include spark::hdfs
}

@Library('merritt-build-library')
import org.cdlib.mrt.build.BuildFunctions;

// See https://github.com/CDLUC3/mrt-jenkins/blob/main/src/org/cdlib/mrt/build/BuildFunctions.groovy

pipeline {
    /*
     * Params:
     *   tagname
     *   purge_local_m2
     */
    environment {      
      //Branch/tag names to incorporate into the build.  Create one var for each repo.
      BRANCH_CORE = 'java-refactor'
      BRANCH_ZK = 'java-refactor'

      //working vars
      M2DIR = "${HOME}/.m2-ingest"
      DEF_BRANCH = "main"
    }
    agent any

    tools {
        // Install the Maven version 3.8.4 and add it to the path.
        maven 'maven384'
    }

    stages {
        stage('Purge Local') {
            steps {
                script {
                  new BuildFunctions().init_build();
                }
            }
        }
        stage('Build Core') {
            steps {
                dir('mrt-core2') {
                  script {
                    new BuildFunctions().build_library(
                      'https://github.com/CDLUC3/mrt-core2.git', 
                      env.BRANCH_CORE, 
                      '-DskipTests'
                    )
                  }
                }
            }
        }
        stage('Build CDL ZK Queue') {
            steps {
                dir('cdl-zk-queue') {
                  script {
                    new BuildFunctions().build_library(
                      'https://github.com/CDLUC3/cdl-zk-queue.git', 
                      env.BRANCH_ZK, 
                      '-DskipTests'
                    )
                  }
                }
            }
        }
        stage('Obsolete dependencies') {
            steps {
                sh "mvn -Dmaven.repo.local=${env.M2DIR} -s ${MAVEN_HOME}/conf/settings.xml dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=jargs -DartifactId=jargs -Dversion=1.1.1 -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/thirdparty/"
                sh "mvn -Dmaven.repo.local=${env.M2DIR} -s ${MAVEN_HOME}/conf/settings.xml dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=org.cdlib.mrt -DartifactId=mrt-dataonesrc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/cdl-snapshots/"
            }
        }
        stage('Build Ingest') {
            steps {
                dir('mrt-ingest'){
                  script {
                    new BuildFunctions().build_war(
                        'https://github.com/CDLUC3/mrt-ingest.git',
                        '-Denforcer.skip=true'
                    )
                  }
                }
            }
        }

        stage('Archive Resources') { // for display purposes
            steps {
                script {
                  new BuildFunctions().save_artifacts(
                    'mrt-ingest/ingest-war/target/mrt-ingestwar-1.0-SNAPSHOT.war',
                    'mrt-ingest'
                  )
                }
            }
        }
    }
}
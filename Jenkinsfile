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
      m2dir = "${HOME}/.m2-ingest"
    }
    agent any

    tools {
        // Install the Maven version 3.8.4 and add it to the path.
        maven 'maven384'
    }

    stages {
        stage('Purge Local') {
            steps {
                sh "echo 'Building tag ${tagname}' > build.current.txt"
                sh "date >> build.current.txt"
                sh "echo '' >> build.current.txt"
                sh "echo 'Purge ${m2dir}: ${remove_local_m2}'"
                script {
                    if (remove_local_m2.toBoolean()) {
                        sh "rm -rf ${m2dir}"
                    }
                }
            }
        }
        stage('Build Core') {
            steps {
                dir('mrt-core2') {
                  git branch: "${env.BRANCH_CORE}", url: 'https://github.com/CDLUC3/mrt-core2.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git branch >> ../build.current.txt"
                  sh "git log --pretty=full -n 1 >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} -s ${MAVEN_HOME}/conf/settings.xml clean install -DskipTests"
                }
            }
        }
        stage('Build CDL ZK') {
            steps {
                dir('cdl-zk-queue') {
                  git branch: "${env.BRANCH_ZK}", url: 'https://github.com/CDLUC3/cdl-zk-queue.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git branch >> ../build.current.txt"
                  sh "git log --pretty=full -n 1 >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} -s ${MAVEN_HOME}/conf/settings.xml clean install -DskipTests"
                }
            }
        }
        stage('Obsolete dependencies') {
            steps {
                sh "mvn -Dmaven.repo.local=${m2dir} -s ${MAVEN_HOME}/conf/settings.xml dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=jargs -DartifactId=jargs -Dversion=1.1.1 -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/thirdparty/"
                sh "mvn -Dmaven.repo.local=${m2dir} -s ${MAVEN_HOME}/conf/settings.xml dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=org.cdlib.mrt -DartifactId=mrt-dataonesrc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/cdl-snapshots/"
            }
        }
        stage('Build Ingest') {
            steps {
                dir('mrt-ingest'){
                  git branch: 'main', url: 'https://github.com/CDLUC3/mrt-ingest.git'
                  checkout([
                        $class: 'GitSCM',
                        branches: [[name: "refs/tags/${tagname}"]],
                  ])
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git branch >> ../build.current.txt"
                  sh "git log --pretty=medium -n 1 >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} -s ${MAVEN_HOME}/conf/settings.xml clean install -Denforcer.skip=true"
                }
            }
        }

        stage('Archive Resources') { // for display purposes
            steps {
                script {
                  sh "cp build.current.txt ${tagname}"
                  archiveArtifacts artifacts: "${tagname}, build.current.txt, mrt-ingest/ingest-war/target/mrt-ingestwar-1.0-SNAPSHOT.war, mrt-ingest/ingest-war/target/mrt-ingestwar-1.0-SNAPSHOT-archive.zip", onlyIfSuccessful: true
                } 
            }
        }
    }
}

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
        // Install the Maven version configured as "M3" and add it to the path.
        maven 'maven384'
    }

    stages {
        stage('Purge Local') {
            steps {
                sh "echo 'Building tag ${tagname}' > build.current.txt"
                sh "echo 'tbd purge ${m2dir}'"
                sh "rm -rf ${m2dir}"
            }
        }
        stage('Build Core') {
            steps {
                dir('mrt-core2') {
                  git branch: "${env.BRANCH_CORE}", url: 'https://github.com/CDLUC3/mrt-core2.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} clean install -DskipTests"
                }
            }
        }
        stage('Build CDL ZK') {
            steps {
                dir('cdl-zk-queue') {
                  git branch: "${env.BRANCH_ZK}", url: 'https://github.com/CDLUC3/cdl-zk-queue.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} clean install -DskipTests"
                }
            }
        }
        stage('Obsolete dependencies') {
            steps {
                sh "mvn -Dmaven.repo.local=${m2dir} dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=jargs -DartifactId=jargs -Dversion=1.1.1 -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/thirdparty/"
                sh "mvn -Dmaven.repo.local=${m2dir} dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=org.cdlib.mrt -DartifactId=mrt-dataonesrc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/cdl-snapshots/"
            }
        }
        stage('Build Ingest') {
            steps {
                dir('mrt-ingest'){
                  checkout([
                        $class: 'GitSCM',
                        url: 'https://github.com/CDLUC3/mrt-ingest.git'
                        branches: [[name: "refs/tags/${tagname}"]],
                  ])
                  //git branch: "${tagname}", url: 'https://github.com/CDLUC3/mrt-ingest.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=${m2dir} clean install -Denforcer.skip=true"
                }
            }
        }

        stage('Archive Resources') { // for display purposes
            steps {
                script {
                  archiveArtifacts artifacts: "build.current.txt, mrt-ingestwar-${major}.${minor}.${patch}.war", onlyIfSuccessful: true
                } 
            }
        }
    }
}

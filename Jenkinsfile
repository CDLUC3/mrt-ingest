pipeline {
    /*
     * build.last.txt - contains the commit hashes for each repo used for the last build that was run
     * rev.last.txt - contains the semantic version for the prior build
     *
     * build.current.txt - contains commit hashes for each repo used in this current build
     * rev.current.txt - calculated semantic version for this build 
     *
     * if the major or minor version changes, reset patch to 0
     * if build.current.txt differs from build.last.txt, increment patch
     */
    environment {
      REV_MAJOR = 1 //manually increment for NON-backwards compatible changes
      REV_MINOR = 0 //manually increment for backwards compatible changes
      REV_PATCH = 0 //build will compute
      
      //Branch/tag names to incorporate into the build.  Create one var for each repo.
      BRANCH_CORE = 'java-refactor'
      BRANCH_ZK = 'java-refactor'
      BRANCH_INGEST = 'java-refactor'

      //working vars
      major = ''
      minor = ''
      patch = ''
    }
    agent any

    tools {
        // Install the Maven version configured as "M3" and add it to the path.
        maven 'maven384'
    }

    stages {
        stage('Last build') { // for display purposes
            steps {
                //sh "env"
                sh "rm -f build.current.txt rev.current.txt mrt-ingestwar-*"
                script {
                  try {
                    sh "curl -s -S -f -o build.last.txt ${JOB_URL}lastSuccessfulBuild/artifact/build.last.txt || touch build.last.txt"
                  } finally {
                  }
                  try {
                    sh "curl -s -S -f -o rev.last.txt ${JOB_URL}lastSuccessfulBuild/artifact/rev.last.txt || echo '${REV_MAJOR} ${REV_MINOR} ${REV_PATCH}' > rev.last.txt"
                  } finally {
                  }
                  major = sh(script: "cut -d' ' -f1 rev.last.txt", returnStdout: true).toString().trim()        
                  minor = sh(script: "cut -d' ' -f2 rev.last.txt", returnStdout: true).toString().trim()        
                  patch = sh(script: "cut -d' ' -f3 rev.last.txt", returnStdout: true).toString().trim()        

                  if (major == '') {
                      major = REV_MAJOR
                  }
                  if (minor == '') {
                      minor = REV_MINOR
                  }
                  if (patch == '') {
                      patch = REV_PATCH
                  }
                }
            }
        }
        stage('Purge Local') {
            steps {
                sh "rm -rf ~/.m2-refactor"
            }
        }
        stage('Build Core') {
            steps {
                dir('mrt-core2') {
                  git branch: "${env.BRANCH_CORE}", url: 'https://github.com/CDLUC3/mrt-core2.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=$HOME/.m2-refactor clean install -DskipTests"
                }
            }
        }
        stage('Build CDL ZK') {
            steps {
                dir('cdl-zk-queue') {
                  git branch: "${env.BRANCH_ZK}", url: 'https://github.com/CDLUC3/cdl-zk-queue.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=$HOME/.m2-refactor clean install -DskipTests"
                }
            }
        }
        stage('Obsolete dependencies') {
            steps {
                sh "mvn -Dmaven.repo.local=$HOME/.m2-refactor dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=jargs -DartifactId=jargs -Dversion=1.1.1 -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/thirdparty/"
                sh "mvn -Dmaven.repo.local=$HOME/.m2-refactor dependency:get -Dmaven.legacyLocalRepo=true -DgroupId=org.cdlib.mrt -DartifactId=mrt-dataonesrc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -DrepoUrl=https://mvn.cdlib.org/content/repositories/cdl-snapshots/"
            }
        }
        stage('Build Ingest') {
            steps {
                dir('mrt-ingest'){
                  git branch: "${env.BRANCH_INGEST}", url: 'https://github.com/CDLUC3/mrt-ingest.git'
                  sh "git remote get-url origin >> ../build.current.txt"
                  sh "git rev-parse HEAD >> ../build.current.txt"
                  sh "mvn -Dmaven.repo.local=$HOME/.m2-refactor clean install -Denforcer.skip=true"
                }
            }
        }

        stage('Compute semantic ver') { // for display purposes
            steps {
                script {
                  if (major < REV_MAJOR || minor < REV_MINOR) {
                      major = REV_MAJOR
                      minor = REV_MINOR
                      patch = 0
                  }

                  try {
                    sh "diff build.last.txt build.current.txt"
                  } catch (err) {
                    sh "echo 'diff found'"
                    patch = patch.toInteger() + 1
                  } 
                  sh "echo ${major} ${minor} ${patch} > rev.current.txt"
                  sh "cp mrt-ingest/ingest-war/target/mrt-ingestwar-1.0-SNAPSHOT.war mrt-ingestwar-${major}.${minor}.${patch}.war"
                }
            }
        }

        stage('Archive Resources') { // for display purposes
            steps {
                script {
                  sh "mv build.current.txt build.last.txt"
                  sh "mv rev.current.txt rev.last.txt"
                  archiveArtifacts artifacts: "build.last.txt, rev.last.txt, mrt-ingestwar-${major}.${minor}.${patch}.war", onlyIfSuccessful: true
                } 
            }
        }
    }
}

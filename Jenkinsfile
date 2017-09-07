#!/usr/bin/env groovy
def labels = ["smartos_dataset_15.4.1"]
def builders = [:]


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {

        stage ('Clean Workspace'){
        	deleteDir()
        }
        
        stage ('Checkout'){
        	checkout scm
        }
        
        BRANCH = sh(returnStdout: true, script: 'git rev-parse --abbrev-ref HEAD').trim()

        stage ('Build'){
			build(BRANCH)
        }
        
        stage ('Upload'){
			def matcher = env.NODE_LABELS =~ 'smartos_dataset_([^ ]+)'
	    	DS_VERSION = matcher[0][1];
	    	matcher = null

	        withAWS(region:'us-east-2', credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        	s3Upload(file:'rel/pkg/artifacts/', bucket:'release-test.project-fifo.net', path:"pkg/${DS_VERSION}/dev/")
			}
        }

        stage ('BuildNotify'){

        }
		
      }
    }
}

try {
	timeout(time=5, unit=MINUTES){
		parallel builders
	}
	notify("SUCCESSFUL")
} catch(err) {
  notify("FAILURE")
  throw err
}






def build (String git_branch) {
    SUFFIX = ""
    if (git_branch != 'origin/master'){
    	SUFFIX = '''
    		export SUFFIX=$(/opt/local/bin/erl -noshell -eval '{{Y, MM, D}, {H, M, S}} = calendar:universal_time(), io:format("pre~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B", [Y, MM, D, H, M, S]),init:stop()');
    	'''
    }

    def EXEC ="""
		export PORTABLE=1
		export TERM=dumb
		export GPG_KEY=BB975564
		${SUFFIX}
		/opt/local/bin/make package 
	    mkdir -p rel/pkg/artifacts
    	cp rel/pkg/*.tgz rel/pkg/artifacts
		mkdir -p rel/pkg/info
		pkg_info -X rel/pkg/*.tgz > rel/pkg/info/\$(pkg_info -X rel/pkg/*.tgz | awk -F "=" '/FILE_NAME/ {print \$2}')
	"""

	sh EXEC
}


def notify(String buildStatus) {
  // build status of null means successful
  buildStatus =  buildStatus ?: 'SUCCESSFUL'

  // Default values
  def colorName = 'RED'
  def colorCode = '#FF0000'
  def subject = "${buildStatus}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'"
  def summary = "${subject} (${env.BUILD_URL})"

  // Override default values based on build status
  if (buildStatus == 'STARTED') {
    color = 'YELLOW'
    colorCode = '#FFFF00'
  } else if (buildStatus == 'SUCCESSFUL') {
    color = 'GREEN'
    colorCode = '#00FF00'
  } else {
    color = 'RED'
    colorCode = '#FF0000'
  }

  // Send notifications
  slackSend (color: colorCode, message: summary)
}

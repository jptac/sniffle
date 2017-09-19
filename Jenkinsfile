#!/usr/bin/env groovy
def labels = ["smartos_dataset_15.4.1"]
def builders = [:]

def s3bucket = 'release-test.project-fifo.net'
def s3Region = 'us-east-2'

properties([[$class: 'BuildDiscarderProperty', 
		strategy: [
		$class: 'LogRotator', 
		artifactDaysToKeepStr: '', 
		artifactNumToKeepStr: '', 
		daysToKeepStr: '', 
		numToKeepStr: '10']
	]]);


for (x in labels) {
    def label = x // Need to bind the label variable before the closure - can't do 'for (label in labels)'

    // Create a map to pass in to the 'parallel' step so we can fire all the builds at once
    builders[label] = {
      node(label) {

      	def matcher = env.NODE_LABELS =~ 'smartos_dataset_([^ ]+)'
	    DS_VERSION = matcher[0][1];
	    matcher = null

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

        	if (BRANCH == 'origin/dev'){
        		withAWS(region: s3Region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/artifacts/', bucket:s3bucket, path:"pkg/${DS_VERSION}/dev/")
				}
        	}
        	else if (BRANCH == 'origin/master'){
        		withAWS(region: s3Region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/artifacts/', bucket:s3bucket, path:"pkg/${DS_VERSION}/rel/")
				}
        	}
        	//No else because we dont publish anything besides dev/rel

        }
        stage ('Publish'){
        	if (BRANCH == 'origin/dev'){
        		publish ('dev', DS_VERSION)
        	}
        	else if (BRANCH == 'origin/master'){
        		publish ('rel', DS_VERSION)
        	}
        	

        	//(
       // 	s3cmd ls s3://release-info.project-fifo.net/pkg/15.4.1/dev/ | awk '{print $4}' | xargs -L1 -I {} s3cmd --no-progress get {} - 2>/dev/null) | bzip2 > fifo.15.4.1.dev.bz2
		//	s3cmd put fifo.15.4.1.dev.bz2 s3://release.project-fifo.net/pkg/15.4.1/dev/pkg_summary.bz2


        }
		
      }
    }
 
}

try {
	timeout(time: 15, unit: 'MINUTES'){
		parallel builders
	}
	notify("SUCCESSFUL")
} catch(err) {
  notify("FAILURE")
  throw err
}



def build (String git_branch) {
	sh 'git fetch --tags'

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

def publish (String rel_dir ,String ds_version) {
	def EXEC ="""
	s3cmd ls s3://release-info.project-fifo.net/pkg/${ds_version}/${rel_dir}/ | awk '{print $4}' | xargs -L1 -I {} s3cmd --no-progress get {} - 2>/dev/null) | bzip2 > pkgsummary.bz2
	s3cmd put pkgsummary.bz2 s3://release.project-fifo.net/pkg/${ds_version}/${rel_dir}/pkg_summary.bz2
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

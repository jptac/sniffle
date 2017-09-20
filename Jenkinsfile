#!/usr/bin/env groovy
def labels = ["smartos_dataset_15.4.1"]
def builders = [:]

s3bucket = 'release.project-fifo.net'
s3infobucket = 'release-info.project-fifo.net'
s3dirprefix = 'test/pkg'
s3region = 'us-east-2'

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

        stage ('Clean Workspace'){
        	deleteDir()
        }
        
        stage ('Checkout'){
        	checkout scm
        }
        
        BRANCH = sh(returnStdout: true, script: 'git ls-remote --heads origin | grep $(git rev-parse HEAD) | cut -d / -f 3').trim()
        def matcher = env.NODE_LABELS =~ 'smartos_dataset_([^ ]+)'
	    DS_VERSION = matcher[0][1];
	    matcher = null

        stage ('Build'){
			build(BRANCH)
        }
        
        stage ('Upload'){

        	if (BRANCH == 'dev'){
        		withAWS(region: s3region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/artifacts/', bucket:s3bucket, path:"${s3dirprefix}/${DS_VERSION}/dev/")
				}
				withAWS(region: s3region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/info/', bucket:s3infobucket, path:"${s3dirprefix}/${DS_VERSION}/dev/")
				}
        	}
        	else if (BRANCH == 'master'){
        		withAWS(region: s3region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/artifacts/', bucket:s3bucket, path:"${s3dirprefix}/${DS_VERSION}/rel/")
				}
				withAWS(region: s3region, credentials:'FifoS3-d54ea704-b99e-4fd1-a9ec-2a3c50e3f2a9') {
	        		s3Upload(file:'rel/pkg/info/', bucket:s3infobucket, path:"${s3dirprefix}/${DS_VERSION}/rel/")
				}
        	}
        	//No else because we dont publish anything besides dev/rel

        }
        stage ('Publish'){
        	if (BRANCH == 'dev'){
        		echo 'publishing dev'
        		publish ('dev', DS_VERSION)
        	}
        	else if (BRANCH == 'master'){
        		echo 'publishing rel'
        		publish ('rel', DS_VERSION)
        	}
        	else {
        		echo 'Skipping publish'
        		echo BRANCH
        	}
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

def publish(String rel_dir, String ds_version) {
	def EXEC ="""
		(s3cmd ls s3://${s3infobucket}/{s3dirprefix}/${ds_version}/${rel_dir}/ | awk '{print \$4}' | xargs -L1 -I {} s3cmd --no-progress get {} - 2>/dev/null) | bzip2 > pkgsummary.bz2
		s3cmd put pkgsummary.bz2 s3://${s3bucket}/{s3dirprefix}/${ds_version}/${rel_dir}/pkg_summary.bz2
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

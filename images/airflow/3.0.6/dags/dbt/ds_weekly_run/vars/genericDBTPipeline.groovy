// Reference: https://dev.to/cwprogram/introduction-to-jenkins-shared-libraries-3p67 

def workspace = pwd() 

// Generates a dbt run or test statement using the given selector. Defaults to running with target = prod.
def dbt_selector(selector_name, run_or_test, target = 'prod') {
	script {
	    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
    	    timeout(time: 10, unit: 'MINUTES') {
        	    sh "dbt ${run_or_test} --selector ${selector_name} --profiles-dir ${workspace}/.dbt --target ${target}"
        	}
    	}
	}	  
}

def call(body) {
     // Possible pipelineParams:
	 	// cronString : used as the cron trigger for the Jenkins job
		// dbtRunSelector : used as the dbt selector for run AND test
    def pipelineParams= [:]
    body.resolveStrategy = Closure.DELEGATE_FIRST
    body.delegate = pipelineParams
    body()

pipeline {
	agent {
		docker {
			image "${env.dbt_version}"
			registryUrl "${env.registry_url}"
			registryCredentialsId 'ecr:us-west-2:aws-iam-role'
			args '''--user root:root \
              -v /var/lib/jenkins/.ssh:/root/.ssh:ro'''
			alwaysPull true
		}
  }

	environment {
		ERROR_MESSAGE = ' '
		SF_ACCOUNT = commonFunctions.get_config('sf_account')
		SF_USER=commonFunctions.get_config('sf_user_kp')
		SF_KEY_PATH=commonFunctions.get_config('sf_key_path')
		SF_ROLE=commonFunctions.get_config('sf_role')
		SF_DATABASE=commonFunctions.get_config('sf_database')
		SF_WAREHOUSE=commonFunctions.get_config('sf_warehouse_prod')
		SF_AWS_KEY=commonFunctions.get_config('sf_aws_key')
		SF_AWS_SECRET=commonFunctions.get_config('sf_aws_secret')
		BRANCH_SCHEMA_NAME="${env.BRANCH_NAME}"
	}

    options {
        disableConcurrentBuilds()
    }

	triggers {
        cron("${pipelineParams.cronString}")
	}

    stages {
		stage('Run DBT') {
			steps {
                dbt_selector(pipelineParams.dbtRunSelector, 'run')
			}
			post {
				failure {
					script {
						sh 'python3 capsaicin/dbt_tools/logger.py parse-results --outfile $PWD/run_out.txt --resource models'
						ERROR_MESSAGE=sh(returnStdout: true, script: 'cat $PWD/run_out.txt').trim()
						sh 'rm $PWD/run_out.txt'
						// Mark the build as failed and end the program
                		error("RUN DBT stage failed: ${ERROR_MESSAGE}")
					}
				}
			}
		}
        stage('Test DBT') {
			steps {
                dbt_selector(pipelineParams.dbtRunSelector, 'test')
			}
			post {
				failure {
					script {
						sh 'python3 capsaicin/dbt_tools/logger.py parse-results --outfile $PWD/run_out.txt --resource models'
						ERROR_MESSAGE=sh(returnStdout: true, script: 'cat $PWD/run_out.txt').trim()
						sh 'rm $PWD/run_out.txt'
						// Mark the build as failed and end the program
                		error("TEST DBT stage failed: ${ERROR_MESSAGE}")
					}
				}
			}
		}
    }
	post {
		success {
			script {
                slackSend(channel: 'team-data-notifications',  color: 'good', message: "Selector ${pipelineParams.dbtRunSelector} run succeeded! \nSee build history on <${BUILD_URL}|Spice Rack AF>.")
			}
		}
		unsuccessful {
			script {
                slackSend(channel: 'team-data-notifications',  color: 'danger', message: "Selector ${pipelineParams.dbtRunSelector} run failed! \nMessage: ${ERROR_MESSAGE} \nSee build history on <${BUILD_URL}|Spice Rack AF>.")
			}
		}
	}
}
}
pipeline {
    agent any

    stages {
        stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                ''' 
            }
        }
        stage('Build') {
            steps {    
                echo 'Building..'
		sh 'go build implog.go' 
           }
        }
	}
}

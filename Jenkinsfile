pipeline {
    agent any

    stages {
        stage('Sqoop Import from PostgreSQL to HDFS') {
            steps {
                sh 'chmod +x src/sqoop_import.sh'
                sh 'bash src/sqoop_import.sh'
            }
        }

        stage('Create Hive Tables') {
            steps {
                sh 'hive -f src/hive_ddl.hql'
            }
        }
    }

    post {
        success {
            echo 'TFL pipeline completed successfully'
        }
        failure {
            echo 'TFL pipeline failed - check logs above'
        }
    }
}

pipeline {
    agent any

    environment {
        PATH = "/usr/local/sqoop/bin:/usr/lib/sqoop/bin:/opt/sqoop/bin:/usr/bin:/usr/local/bin:${env.PATH}"
    }

    stages {
        stage('Check Environment') {
            steps {
                sh 'which sqoop || find /usr -name sqoop 2>/dev/null | head -5'
                sh 'which hive || find /usr -name hive 2>/dev/null | head -5'
            }
        }

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

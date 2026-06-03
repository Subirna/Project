pipeline {
    agent any

    environment {
        CLOUDERA_HOST = '13.41.167.97'
        CLOUDERA_USER = 'consultant'
        REMOTE_DIR    = '/home/consultant/subirna/TFL_Project'
    }

    stages {
        stage('Prepare Remote Directory') {
            steps {
                sh "ssh ${CLOUDERA_USER}@${CLOUDERA_HOST} mkdir -p ${REMOTE_DIR}/sqoop ${REMOTE_DIR}/hive"
            }
        }

        stage('Copy Scripts to Cloudera') {
            steps {
                sh "scp src/sqoop_import.sh ${CLOUDERA_USER}@${CLOUDERA_HOST}:${REMOTE_DIR}/sqoop/"
                sh "scp src/hive_ddl.hql   ${CLOUDERA_USER}@${CLOUDERA_HOST}:${REMOTE_DIR}/hive/"
            }
        }

        stage('Set Permissions') {
            steps {
                sh "ssh ${CLOUDERA_USER}@${CLOUDERA_HOST} chmod +x ${REMOTE_DIR}/sqoop/sqoop_import.sh"
            }
        }

        stage('Sqoop Import from PostgreSQL to HDFS') {
            steps {
                sh "ssh ${CLOUDERA_USER}@${CLOUDERA_HOST} bash ${REMOTE_DIR}/sqoop/sqoop_import.sh"
            }
        }

        stage('Create Hive Tables') {
            steps {
                sh "ssh ${CLOUDERA_USER}@${CLOUDERA_HOST} hive -f ${REMOTE_DIR}/hive/hive_ddl.hql"
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

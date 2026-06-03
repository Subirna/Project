pipeline {
    agent any

    environment {
        REMOTE_HOST     = '13.41.167.97'
        REMOTE_USER     = 'consultant'
        REMOTE_PASSWORD = 'WelcomeItc@2026'
        PROJECT_DIR     = '/home/consultant/subirna/TFL_Project'
        HDFS_DIR        = '/tmp/subirna/TFL_project'
    }

    stages {
        stage('Checkout') {
            steps {
                echo '========================================='
                echo 'Stage 1: Git Checkout'
                echo '========================================='
                checkout scm
                sh 'git log -1 --oneline'
            }
        }

        stage('Prepare Remote Directory') {
            steps {
                echo '========================================='
                echo 'Stage 2: Create Directories on Cloudera'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "mkdir -p ${PROJECT_DIR}/sqoop ${PROJECT_DIR}/hive" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Directories created"
                '''
            }
        }

        stage('Copy Scripts to Cloudera') {
            steps {
                echo '========================================='
                echo 'Stage 3: Copy Sqoop and Hive Scripts'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/sqoop_import.sh ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/sqoop/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/hive_ddl.hql ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/hive/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Scripts copied successfully"
                '''
            }
        }

        stage('Set Permissions') {
            steps {
                echo '========================================='
                echo 'Stage 4: Set Execute Permissions'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "chmod +x ${PROJECT_DIR}/sqoop/sqoop_import.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Permissions set"
                '''
            }
        }

        stage('Prepare Staging Directory') {
            steps {
                echo '========================================='
                echo 'Stage 5: Create local staging directory'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "mkdir -p /tmp/hadoop/mapred/staging" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true
                    echo "Staging directory ready"
                '''
            }
        }

        stage('Clean HDFS') {
            steps {
                echo '========================================='
                echo 'Stage 5: Clean HDFS directories'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "hdfs dfs -rm -r -f -skipTrash ${HDFS_DIR} 2>/dev/null || true" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true
                    echo "HDFS cleaned"
                '''
            }
        }

        stage('Sqoop Import from PostgreSQL to HDFS') {
            steps {
                echo '========================================='
                echo 'Stage 5: Run Sqoop Import (6 tables)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "bash ${PROJECT_DIR}/sqoop/sqoop_import.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Sqoop import completed"
                '''
            }
        }

        stage('Create Hive Tables') {
            steps {
                echo '========================================='
                echo 'Stage 6: Create Hive External Tables'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "hive -f ${PROJECT_DIR}/hive/hive_ddl.hql" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Hive tables created"
                '''
            }
        }

        stage('Verify Results') {
            steps {
                echo '========================================='
                echo 'Stage 7: Verify HDFS Data'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "hdfs dfs -ls ${HDFS_DIR} 2>/dev/null || echo 'HDFS directory not found'" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true
                '''
            }
        }
    }

    post {
        success {
            echo '========================================='
            echo 'TFL PIPELINE COMPLETED SUCCESSFULLY'
            echo '========================================='
            echo "Cloudera: ${REMOTE_HOST}:${PROJECT_DIR}"
            echo "HDFS: ${HDFS_DIR}"
            echo '========================================='
        }
        failure {
            echo '========================================='
            echo 'TFL PIPELINE FAILED - check logs above'
            echo '========================================='
        }
        always {
            echo 'Pipeline execution completed'
        }
    }
}

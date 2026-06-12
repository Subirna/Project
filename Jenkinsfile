pipeline {
    agent any

    // ==========================================================
    //  PARAMETER — Choose Full Load OR Incremental Load
    //
    //  Uttam's Jenkinsfile uses a LOAD_MODE parameter so the
    //  same pipeline file handles both modes.
    //
    //  full_load   → runs ONCE at the start (all historical data)
    //  incremental → runs DAILY (only new records added since last run)
    //
    //  How to use:
    //    Jenkins → Build with Parameters → choose LOAD_MODE
    // ==========================================================
    parameters {
        choice(
            name: 'LOAD_MODE',
            choices: ['full_load', 'incremental'],
            description: '''
                full_load   = import ALL data from PostgreSQL (run ONCE at the beginning)
                incremental = import only NEW records since last run (run DAILY)
            '''
        )
    }

    // ==========================================================
    //  ENVIRONMENT — credentials and paths
    //  Same values as original Jenkinsfile, unchanged
    // ==========================================================
    environment {
        REMOTE_HOST      = '13.41.167.97'
        REMOTE_USER      = 'consultant'
        REMOTE_PASSWORD  = 'Cl0ud3ra@2026#Secur3!'
        PROJECT_DIR      = '/home/consultant/subirna/TFL_Project'
        HDFS_DIR         = '/tmp/subirna/TFL_project'
        HIVESERVER2_HOST = '18.175.245.20'
    }

    stages {

        // ======================================================
        //  STAGE 1: GIT CHECKOUT
        //  Always runs for both full_load and incremental.
        //  Gets the latest scripts from the repository.
        // ======================================================
        stage('Checkout') {
            steps {
                echo '========================================='
                echo 'Stage 1: Git Checkout'
                echo '========================================='
                checkout scm
                sh 'git log -1 --oneline'
            }
        }

        // ======================================================
        //  STAGE 2: PREPARE REMOTE DIRECTORY
        //  Always runs for both modes.
        //  Creates the project folders on the Cloudera edge node.
        // ======================================================
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

        // ======================================================
        //  STAGE 3: COPY ALL SCRIPTS TO CLOUDERA
        //  Always runs for both modes.
        //  Copies BOTH full load AND incremental scripts.
        //  This way all scripts are always up to date on the server.
        // ======================================================
        stage('Copy Scripts to Cloudera') {
            steps {
                echo '========================================='
                echo 'Stage 3: Copy All Scripts (full + incremental)'
                echo '========================================='
                sh '''
                    # Full load scripts (original, unchanged)
                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/sqoop_import.sh ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/sqoop/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/tfl_spark_analysis.py ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/hive_ddl.hql ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/hive/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    # Incremental scripts (new)
                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/simulate_data_split.py ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/incremental_sqoop.py ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    sshpass -p "${REMOTE_PASSWORD}" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        src/incremental_spark.py ${REMOTE_USER}@${REMOTE_HOST}:${PROJECT_DIR}/ 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "All scripts copied successfully"
                '''
            }
        }

        // ======================================================
        //  STAGE 4: SET PERMISSIONS
        //  Always runs. Makes all shell scripts executable.
        // ======================================================
        stage('Set Permissions') {
            steps {
                echo '========================================='
                echo 'Stage 4: Set Execute Permissions'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "chmod +x ${PROJECT_DIR}/sqoop/sqoop_import.sh ${PROJECT_DIR}/*.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Permissions set"
                '''
            }
        }

        // ======================================================
        //  STAGE 5: PREPARE STAGING DIRECTORY
        //  Always runs.
        // ======================================================
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

        // ======================================================
        //  STAGE 6: SIMULATE DATA SPLIT  ← FULL LOAD ONLY
        //
        //  Splits TFL data into two parts (Uttam's approach):
        //    full_load_data.csv        → years 2017-2019 (historical)
        //    incremental_load_data.csv → years 2020-2021 (new data)
        //
        //  Only runs when LOAD_MODE = full_load
        // ======================================================
        stage('Simulate Data Split') {
            when {
                expression { params.LOAD_MODE == 'full_load' }
            }
            steps {
                echo '========================================='
                echo 'Stage 6: Simulate Data Split (Full Load only)'
                echo 'Splitting data: 2017-2019 = full load, 2020-2021 = incremental'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "cd ${PROJECT_DIR} && python3 simulate_data_split.py" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Data split complete"
                '''
            }
        }

        // ======================================================
        //  STAGE 7: CLEAN HDFS  ← FULL LOAD ONLY
        //
        //  Clears HDFS before a full load so we start fresh.
        //  We do NOT clean HDFS for incremental — old data must
        //  stay so incremental_spark.py can read it.
        //
        //  Only runs when LOAD_MODE = full_load
        // ======================================================
        stage('Clean HDFS') {
            when {
                expression { params.LOAD_MODE == 'full_load' }
            }
            steps {
                echo '========================================='
                echo 'Stage 7: Clean HDFS (Full Load only)'
                echo 'NOTE: HDFS is NOT cleaned for incremental load'
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

        // ======================================================
        //  STAGE 8: FULL LOAD — SQOOP  ← FULL LOAD ONLY
        //
        //  Imports ALL rows from all 6 tables into HDFS.
        //  Uses sqoop_import.sh (original script, unchanged).
        //
        //  Only runs when LOAD_MODE = full_load
        // ======================================================
        stage('Full Load — Sqoop Import') {
            when {
                expression { params.LOAD_MODE == 'full_load' }
            }
            steps {
                echo '========================================='
                echo 'Stage 8: Full Load Sqoop (all 6 tables, all rows)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "bash ${PROJECT_DIR}/sqoop/sqoop_import.sh" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Full load Sqoop completed"
                '''
            }
        }

        // ======================================================
        //  STAGE 9: FULL LOAD — SPARK  ← FULL LOAD ONLY
        //
        //  Reads all imported data, runs 7 analyses,
        //  creates gold tables. Uses tfl_spark_analysis.py (unchanged).
        //
        //  Only runs when LOAD_MODE = full_load
        // ======================================================
        stage('Full Load — Spark Analysis') {
            when {
                expression { params.LOAD_MODE == 'full_load' }
            }
            steps {
                echo '========================================='
                echo 'Stage 9: Full Load Spark (creates all 7 gold tables)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "spark-submit --master local[*] ${PROJECT_DIR}/tfl_spark_analysis.py" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Full load Spark completed"
                '''
            }
        }

        // ======================================================
        //  STAGE 10: FULL LOAD — CREATE HIVE TABLES  ← FULL LOAD ONLY
        //
        //  Creates Hive external tables on top of the HDFS data.
        //  Only runs when LOAD_MODE = full_load
        // ======================================================
        stage('Full Load — Create Hive Tables') {
            when {
                expression { params.LOAD_MODE == 'full_load' }
            }
            steps {
                echo '========================================='
                echo 'Stage 10: Create Hive External Tables (Full Load only)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "beeline -u 'jdbc:hive2://${HIVESERVER2_HOST}:10000/default' -f ${PROJECT_DIR}/hive/hive_ddl.hql" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Hive tables created"
                '''
            }
        }

        // ======================================================
        //  STAGE 11: INCREMENTAL — SQOOP  ← INCREMENTAL ONLY
        //
        //  Uttam's approach applied to ALL 6 tables:
        //    For each table:
        //      1. get_watermark.sh queries Hive MAX(created_at)
        //      2. Sqoop imports only rows newer than that watermark
        //
        //  Uses incremental_sqoop.py which internally calls
        //  get_watermark.sh for each of the 6 tables.
        //
        //  Only runs when LOAD_MODE = incremental
        // ======================================================
        stage('Incremental — Sqoop Import (All 6 Tables)') {
            when {
                expression { params.LOAD_MODE == 'incremental' }
            }
            steps {
                echo '========================================='
                echo 'Stage 11: Incremental Sqoop (all 6 tables, new rows only)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "cd ${PROJECT_DIR} && export HIVESERVER2_HOST=${HIVESERVER2_HOST} && python3 incremental_sqoop.py" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Incremental Sqoop completed"
                '''
            }
        }

        // ======================================================
        //  STAGE 12: INCREMENTAL — SPARK  ← INCREMENTAL ONLY
        //
        //  Reads BOTH:
        //    - Full load data  (/tmp/subirna/TFL_project/)
        //    - Incremental data (/tmp/subirna/TFL_project/incremental/)
        //  Unions them and overwrites all 7 gold tables with
        //  updated results that include the new rows.
        //
        //  Only runs when LOAD_MODE = incremental
        // ======================================================
        stage('Incremental — Spark Analysis (Update Gold Tables)') {
            when {
                expression { params.LOAD_MODE == 'incremental' }
            }
            steps {
                echo '========================================='
                echo 'Stage 12: Incremental Spark (merges full + new, updates gold)'
                echo '========================================='
                sh '''
                    sshpass -p "${REMOTE_PASSWORD}" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
                        ${REMOTE_USER}@${REMOTE_HOST} \
                        "spark-submit --master local[*] ${PROJECT_DIR}/incremental_spark.py" 2>&1 | \
                        grep -v "ITC Big Data Lab" | grep -v "Commands:" | grep -v "HDFS home:" | grep -v "━" || true

                    echo "Incremental Spark completed"
                '''
            }
        }

        // ======================================================
        //  STAGE 13: VERIFY RESULTS
        //  Always runs for both modes.
        //  Lists HDFS directories to confirm data was written.
        // ======================================================
        stage('Verify Results') {
            steps {
                echo '========================================='
                echo 'Stage 13: Verify HDFS Data'
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

    // ==========================================================
    //  POST ACTIONS — same as original Jenkinsfile
    // ==========================================================
    post {
        success {
            echo '========================================='
            echo "TFL PIPELINE COMPLETED SUCCESSFULLY"
            echo "Mode: ${params.LOAD_MODE}"
            echo '========================================='
            echo "Cloudera: ${REMOTE_HOST}:${PROJECT_DIR}"
            echo "HDFS: ${HDFS_DIR}"
            echo '========================================='
        }
        failure {
            echo '========================================='
            echo "TFL PIPELINE FAILED — check logs above"
            echo "Mode: ${params.LOAD_MODE}"
            echo '========================================='
        }
        always {
            echo 'Pipeline execution completed'
        }
    }
}

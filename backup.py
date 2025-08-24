#!/usr/bin/env python3
import os
import shutil
import boto3
from datetime import datetime
import schedule
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLiteBackup:
    def __init__(self):
        self.sqlite_path = os.getenv('SQLITE_PATH')
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = os.getenv('S3_PREFIX', 'sqlite-backups')
        self.cron_schedule = os.getenv('CRON_SCHEDULE', '0 * * * *')  # hourly
        
        if not self.sqlite_path:
            raise ValueError("SQLITE_PATH environment variable is required")
        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is required")
        
        # S3 client setup
        self.s3 = boto3.client(
            's3',
            endpoint_url=os.getenv('S3_ENDPOINT'),
            aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
            region_name=os.getenv('S3_REGION', 'us-east-1')
        )
    
    def backup_sqlite(self):
        timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
        backup_file = f'/tmp/backup-{timestamp}.db'
        
        try:
            # Check if SQLite file exists
            if not os.path.exists(self.sqlite_path):
                logger.error(f"SQLite file not found: {self.sqlite_path}")
                return
            
            logger.info(f"Creating SQLite backup from {self.sqlite_path}")
            
            # Create a consistent copy of the SQLite file
            shutil.copy2(self.sqlite_path, backup_file)
            
            # Upload to S3
            s3_key = f"{self.s3_prefix}/backup-{timestamp}.db"
            logger.info(f"Uploading to s3://{self.s3_bucket}/{s3_key}")
            
            self.s3.upload_file(backup_file, self.s3_bucket, s3_key)
            
            # Cleanup local backup file
            os.remove(backup_file)
            logger.info("Backup completed successfully")
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            # Cleanup on failure
            if os.path.exists(backup_file):
                os.remove(backup_file)
    
    def run(self):
        logger.info(f"Starting SQLite backup service (schedule: {self.cron_schedule})")
        logger.info(f"Monitoring SQLite file: {self.sqlite_path}")
        
        # Parse cron schedule (simplified - just support hourly/daily for now)
        if self.cron_schedule == '0 * * * *':  # hourly
            schedule.every().hour.do(self.backup_sqlite)
        elif self.cron_schedule == '0 0 * * *':  # daily
            schedule.every().day.at("00:00").do(self.backup_sqlite)
        else:
            # Default to hourly if can't parse
            schedule.every().hour.do(self.backup_sqlite)
        
        # Run initial backup
        self.backup_sqlite()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == '__main__':
    backup = SQLiteBackup()
    backup.run()

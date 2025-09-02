import boto3
import requests
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp
from datetime import datetime, timedelta
import json

# Initialize boto3 client for the new bucket
s3 = boto3.client(
    's3',
    aws_access_key_id='access',
    aws_secret_access_key='secret',
    region_name='us-east-1'
)

# Define paths and configurations
database_name = "calendly_bronze"
table_name = "raw_campaign_data"
file_index_url = "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/file_index.json"
base_url = "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/"

def get_available_files():
    """Get list of available JSON files from the file index"""
    try:
        print("📋 Getting available files from file index...")
        
        # Download the file index
        response = requests.get(file_index_url)
        response.raise_for_status()
        
        file_index = response.json()
        print(f"✅ File index loaded: {len(file_index.get('files', []))} files found")
        
        # Get list of available files
        available_files = file_index.get('files', [])
        
        print("Available files:")
        for i, file_info in enumerate(available_files[:10]):  # Show first 10
            print(f"  {i+1}. {file_info['name']} (updated: {file_info.get('last_modified', 'N/A')})")
        if len(available_files) > 10:
            print(f"  ... and {len(available_files) - 10} more files")
            
        return available_files
        
    except Exception as e:
        print(f"❌ Error getting file index: {e}")
        
        # Fallback: Generate file names for the past 7 days
        print("🔄 Falling back to date-based file generation...")
        return generate_date_based_files()

def generate_date_based_files():
    """Generate file names based on dates (fallback)"""
    files = []
    today = datetime.now()
    
    for i in range(1, 8):  # Last 7 days
        date_str = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        file_name = f"spend_data_{date_str}.json"
        files.append({
            'name': file_name,
            'url': f"{base_url}{file_name}",
            'fallback_generated': True
        })
    
    print(f"Generated {len(files)} date-based file names")
    return files

def download_campaign_data(available_files):
    """Download and process campaign data JSON files"""
    try:
        print("📥 Downloading campaign data files...")
        
        json_data = []
        successful_downloads = 0
        
        for i, file_info in enumerate(available_files):
            file_name = file_info['name']
            file_url = file_info.get('url', f"{base_url}{file_name}")
            
            try:
                print(f"Downloading {i+1}/{len(available_files)}: {file_name}")
                
                # Download the JSON file
                response = requests.get(file_url)
                response.raise_for_status()
                
                content = response.text
                
                json_data.append({
                    'json_content': content,
                    'source_file': file_name,
                    'file_url': file_url,
                    'file_size': len(content),
                    'download_timestamp': datetime.now().isoformat(),
                    'is_fallback': file_info.get('fallback_generated', False)
                })
                
                successful_downloads += 1
                print(f"✅ Success: {file_name}")
                
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Failed to download {file_name}: {e}")
                continue
            except Exception as e:
                print(f"⚠️ Error processing {file_name}: {e}")
                continue
        
        print(f"✅ Successfully downloaded {successful_downloads}/{len(available_files)} files")
        return json_data
        
    except Exception as e:
        print(f"❌ Error downloading campaign data: {e}")
        return []

def setup_database():
    """Create database and table if they don't exist"""
    try:
        # Create database if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        spark.sql(f"USE {database_name}")
        print(f"✅ Database '{database_name}' is ready")
        
        return True
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False

def create_campaign_dataframe(json_data):
    """Create DataFrame with campaign JSON content"""
    try:
        if not json_data:
            print("❌ No campaign data to process")
            return None
        
        # Create Spark DataFrame
        rows = []
        for item in json_data:
            rows.append(Row(
                raw_json=item['json_content'],
                source_file=item['source_file'],
                file_url=item['file_url'],
                file_size_bytes=item['file_size'],
                download_timestamp=item['download_timestamp'],
                is_fallback_generated=item.get('is_fallback', False)
            ))
        
        df = spark.createDataFrame(rows)
        
        print("📊 Campaign DataFrame created")
        print("📋 Schema:")
        df.printSchema()
        
        print(f"✅ Total campaign records: {df.count()}")
        df.show(5, truncate=True)
        
        return df
        
    except Exception as e:
        print(f"❌ Error creating campaign DataFrame: {e}")
        return None

def create_campaign_table(df):
    """Create Delta table for campaign data"""
    try:
        if df is None:
            print("❌ No campaign data to write")
            return False
        
        print(f"💾 Creating campaign table: {database_name}.{table_name}")
        
        # Write to Delta table
        df.write \
          .format("delta") \
          .mode("append") \
          .saveAsTable(f"{database_name}.{table_name}")
        
        print("✅ Campaign Delta table created successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error creating campaign table: {e}")
        return False

def verify_campaign_data():
    """Verify the campaign data was stored correctly"""
    try:
        print("🔍 Verifying campaign data storage...")
        
        # Check if table exists
        tables = spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}'")
        if tables.count() == 0:
            print("❌ Campaign table doesn't exist")
            return
        
        # Count records
        count_df = spark.sql(f"SELECT COUNT(*) as total_records FROM {database_name}.{table_name}")
        record_count = count_df.collect()[0]['total_records']
        print(f"✅ Total campaign records: {record_count:,}")
        
        # Show metadata summary
        print("\n📊 Campaign data summary:")
        spark.sql(f"""
            SELECT 
                COUNT(*) as total_files,
                SUM(file_size_bytes) as total_size_bytes,
                SUM(file_size_bytes) / 1024 / 1024 as total_size_mb,
                SUM(CASE WHEN is_fallback_generated THEN 1 ELSE 0 END) as fallback_files
            FROM {database_name}.{table_name}
        """).show(truncate=False)
        
        # Show sample files
        print("\n📁 Sample campaign files:")
        spark.sql(f"""
            SELECT 
                source_file,
                file_size_bytes,
                LENGTH(raw_json) as json_length,
                is_fallback_generated
            FROM {database_name}.{table_name}
            LIMIT 5
        """).show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error verifying campaign data: {e}")

def campaign_data_pipeline():
    """Complete pipeline for campaign data ingestion"""
    
    print("🚀 Starting campaign data ingestion pipeline...")
    print("=" * 60)
    
    # 1. Get available files
    available_files = get_available_files()
    if not available_files:
        print("❌ No files available for processing")
        return
    
    # 2. Download campaign data
    campaign_data = download_campaign_data(available_files)
    if not campaign_data:
        print("❌ Failed to download campaign data")
        return
    
    # 3. Setup database
    if not setup_database():
        print("❌ Failed to setup database")
        return
    
    # 4. Create DataFrame
    campaign_df = create_campaign_dataframe(campaign_data)
    if campaign_df is None:
        print("❌ Failed to create DataFrame")
        return
    
    # 5. Create Delta table
    success = create_campaign_table(campaign_df)
    
    # 6. Verify
    if success:
        verify_campaign_data()
        print("🎉 Campaign data ingestion completed successfully!")
        
        # Show next steps
        print("\n" + "=" * 60)
        print("📝 Next steps:")
        print("   - Your raw campaign data is in: calendly_bronze.raw_campaign_data")
        print("   - Your webhook data is in: calendly_bronze.raw_webhooks")
        print("   - You can now join or analyze both datasets together")
        
    else:
        print("❌ Campaign data pipeline completed with errors")

# Run the campaign data pipeline
campaign_data_pipeline()
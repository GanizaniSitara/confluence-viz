#!/usr/bin/env python3
"""
Check statistics for documents in a knowledge collection.
Shows counts from both PostgreSQL and Qdrant to verify upload status.
"""
import argparse
import configparser
import json
import os
import psycopg2
from qdrant_client import QdrantClient
from datetime import datetime
import sys

# Detect if we're in WSL
is_wsl = os.path.exists('/proc/version') and 'microsoft' in open('/proc/version').read().lower()

def load_config():
    """Load configuration from settings.ini"""
    config = configparser.ConfigParser()
    config_file = 'settings.ini'
    
    if not os.path.exists(config_file):
        print(f"Error: Configuration file '{config_file}' not found.")
        print("Please create it from settings.example.ini")
        sys.exit(1)
    
    config.read(config_file)
    
    settings = {}
    
    # Load settings from qdrant_uploader section
    if 'qdrant_uploader' in config:
        for key, value in config['qdrant_uploader'].items():
            settings[key] = value
    else:
        print("Error: Missing [qdrant_uploader] section in settings.ini")
        sys.exit(1)
    
    # Convert types
    settings['qdrant_port'] = int(settings.get('qdrant_port', 6333))
    settings['db_port'] = int(settings.get('db_port', 5432))
    
    return settings

def check_postgresql_stats(config):
    """Check document statistics in PostgreSQL"""
    print("\n" + "="*60)
    print("PostgreSQL Statistics")
    print("="*60)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config['db_host'],
            port=config['db_port'],
            database=config['db_name'],
            user=config['db_user'],
            password=config['db_password']
        )
        cur = conn.cursor()
        
        # Get knowledge collection info
        knowledge_id = config['knowledge_id']
        cur.execute("""
            SELECT name, description, created_at, updated_at, data, meta
            FROM knowledge 
            WHERE id = %s
        """, (knowledge_id,))
        
        knowledge_info = cur.fetchone()
        if not knowledge_info:
            print(f"Knowledge collection '{knowledge_id}' not found!")
            return
        
        name, description, created_at, updated_at, data, meta = knowledge_info
        
        print(f"\nKnowledge Collection: {name}")
        print(f"ID: {knowledge_id}")
        if description:
            print(f"Description: {description}")
        
        # Convert timestamps
        created_date = datetime.fromtimestamp(created_at/1000).strftime('%Y-%m-%d %H:%M:%S') if created_at else 'Unknown'
        updated_date = datetime.fromtimestamp(updated_at/1000).strftime('%Y-%m-%d %H:%M:%S') if updated_at else 'Unknown'
        print(f"Created: {created_date}")
        print(f"Last Updated: {updated_date}")
        
        # Count files in knowledge.data
        file_count_from_data = 0
        if data and 'files' in data:
            file_count_from_data = len(data['files'])
            print(f"\nFiles in knowledge.data: {file_count_from_data}")
            
            # Show sample of files
            if file_count_from_data > 0:
                print("\nSample files (first 5):")
                for i, file_info in enumerate(data['files'][:5]):
                    print(f"  {i+1}. {file_info.get('filename', 'Unknown')} (ID: {file_info.get('id', 'Unknown')[:8]}...)")
        
        # Count actual files in file table with this knowledge_id
        cur.execute("""
            SELECT COUNT(*) 
            FROM file 
            WHERE meta->>'knowledge_id' = %s
        """, (knowledge_id,))
        
        file_count_in_table = cur.fetchone()[0]
        print(f"\nFiles in database with this knowledge_id: {file_count_in_table}")
        
        # Get file statistics
        if file_count_in_table > 0:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_files,
                    SUM((meta->>'size')::int) as total_size,
                    MIN(created_at) as oldest,
                    MAX(created_at) as newest
                FROM file 
                WHERE meta->>'knowledge_id' = %s
            """, (knowledge_id,))
            
            stats = cur.fetchone()
            total_files, total_size, oldest, newest = stats
            
            if total_size:
                size_mb = total_size / (1024 * 1024)
                print(f"Total size: {size_mb:.2f} MB")
            
            if oldest:
                oldest_date = datetime.fromtimestamp(oldest/1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"Oldest file: {oldest_date}")
            
            if newest:
                newest_date = datetime.fromtimestamp(newest/1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"Newest file: {newest_date}")
            
            # Count by source
            cur.execute("""
                SELECT 
                    COALESCE(meta->>'source', 'unknown') as source,
                    COUNT(*) as count
                FROM file 
                WHERE meta->>'knowledge_id' = %s
                GROUP BY meta->>'source'
                ORDER BY count DESC
            """, (knowledge_id,))
            
            sources = cur.fetchall()
            if sources:
                print("\nFiles by source:")
                for source, count in sources:
                    print(f"  {source}: {count}")
            
            # For Confluence files, count by space
            cur.execute("""
                SELECT 
                    meta->>'space_key' as space_key,
                    COUNT(*) as count
                FROM file 
                WHERE meta->>'knowledge_id' = %s
                  AND meta->>'source' = 'confluence'
                  AND meta->>'space_key' IS NOT NULL
                GROUP BY meta->>'space_key'
                ORDER BY count DESC
            """, (knowledge_id,))
            
            spaces = cur.fetchall()
            if spaces:
                print("\nConfluence pages by space:")
                for space_key, count in spaces:
                    print(f"  {space_key}: {count} pages")
        
        # Check for any files belonging to this user
        user_id = config.get('user_id')
        if user_id:
            cur.execute("""
                SELECT COUNT(*) 
                FROM file 
                WHERE user_id = %s
            """, (user_id,))
            
            user_file_count = cur.fetchone()[0]
            print(f"\nTotal files for user {user_id}: {user_file_count}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")

def check_qdrant_stats(config):
    """Check vector statistics in Qdrant"""
    print("\n" + "="*60)
    print("Qdrant Vector Database Statistics")
    print("="*60)
    
    try:
        # Connect to Qdrant
        client = QdrantClient(host=config['qdrant_host'], port=config['qdrant_port'])
        
        # Check collections
        collections = client.get_collections().collections
        collection_names = [col.name for col in collections]
        
        print(f"\nAvailable collections: {len(collection_names)}")
        for name in collection_names:
            if 'webui' in name.lower():
                print(f"  - {name}")
        
        # Check knowledge collection
        knowledge_collection = "open-webui_knowledge"
        if knowledge_collection in collection_names:
            collection_info = client.get_collection(knowledge_collection)
            print(f"\n{knowledge_collection}:")
            print(f"  Total points: {collection_info.points_count}")
            print(f"  Vector size: {collection_info.config.params.vectors.size}")
            
            # Count points for this knowledge_id
            knowledge_id = config['knowledge_id']
            
            # Search for points with this knowledge_id
            from qdrant_client.models import Filter, FieldCondition, MatchValue
            
            try:
                result = client.scroll(
                    collection_name=knowledge_collection,
                    scroll_filter=Filter(
                        must=[
                            FieldCondition(
                                key="tenant_id",
                                match=MatchValue(value=knowledge_id)
                            )
                        ]
                    ),
                    limit=1,
                    with_payload=False,
                    with_vectors=False
                )
                
                # Get total count
                count = 0
                next_offset = result[1]
                count += len(result[0])
                
                while next_offset:
                    result = client.scroll(
                        collection_name=knowledge_collection,
                        scroll_filter=Filter(
                            must=[
                                FieldCondition(
                                    key="tenant_id",
                                    match=MatchValue(value=knowledge_id)
                                )
                            ]
                        ),
                        limit=100,
                        offset=next_offset,
                        with_payload=False,
                        with_vectors=False
                    )
                    count += len(result[0])
                    next_offset = result[1]
                
                print(f"  Chunks for knowledge_id '{knowledge_id}': {count}")
                
            except Exception as e:
                print(f"  Could not count chunks for knowledge_id: {e}")
        
        # Check files collection
        files_collection = "open-webui_files"
        if files_collection in collection_names:
            collection_info = client.get_collection(files_collection)
            print(f"\n{files_collection}:")
            print(f"  Total points: {collection_info.points_count}")
            
    except Exception as e:
        print(f"Error connecting to Qdrant: {e}")

def main():
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="Check statistics for documents in knowledge collection"
    )
    parser.add_argument("--knowledge-id", 
                       help="Override knowledge ID from settings.ini")
    parser.add_argument("--user-id",
                       help="Override user ID from settings.ini")
    parser.add_argument("--postgres-only", action="store_true",
                       help="Only check PostgreSQL, skip Qdrant")
    parser.add_argument("--qdrant-only", action="store_true",
                       help="Only check Qdrant, skip PostgreSQL")
    
    args = parser.parse_args()
    
    # Override config if provided
    if args.knowledge_id:
        config['knowledge_id'] = args.knowledge_id
    if args.user_id:
        config['user_id'] = args.user_id
    
    # Validate required settings
    if 'knowledge_id' not in config:
        print("Error: knowledge_id not found in settings.ini")
        sys.exit(1)
    
    print("="*60)
    print("Knowledge Collection Statistics")
    print("="*60)
    print(f"Knowledge ID: {config['knowledge_id']}")
    if 'user_id' in config:
        print(f"User ID: {config['user_id']}")
    
    # Check PostgreSQL
    if not args.qdrant_only:
        check_postgresql_stats(config)
    
    # Check Qdrant
    if not args.postgres_only:
        check_qdrant_stats(config)
    
    print("\n" + "="*60)
    print("Check complete!")
    print("="*60)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3

import argparse
import json
import boto3
import sys
from typing import Any
from botocore.exceptions import ClientError

def load_lifecycle_config(config_path: str) -> dict[str, Any]:
    try:
        with open(config_path, 'r') as f:
            result: dict[str, Any] = json.load(f)
            return result
    except json.JSONDecodeError:
        print(f"Error: {config_path} contains invalid JSON")
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: Could not find file {config_path}")
        sys.exit(1)

def print_lifecycle_rules(rules: list[dict[str, Any]]) -> None:
    if not rules:
        print("No lifecycle rules configured")
        return
        
    for rule in rules:
        print(f"- {rule['ID']}")
        print(f"  Status: {rule['Status']}")
        if 'Expiration' in rule:
            print(f"  Expiration: {rule['Expiration'].get('Days', 'N/A')} days")
        print()

def get_current_rules(s3: Any, bucket_name: str) -> list[dict[str, Any]]:
    try:
        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        rules: list[dict[str, Any]] = response.get('Rules', [])
        return rules
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        raise

def apply_lifecycle_rules(bucket_name: str, lifecycle_config: dict[str, Any]) -> None:
    s3 = boto3.client('s3')
    
    try:
        # First verify the bucket exists and we have access
        s3.head_bucket(Bucket=bucket_name)
        
        # Show current configuration
        print(f"\nCurrent lifecycle rules for bucket {bucket_name}:")
        current_rules = get_current_rules(s3, bucket_name)
        print_lifecycle_rules(current_rules)
        
        # Apply the new configuration
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config  # type: ignore[arg-type]
        )
        print(f"\nSuccessfully applied new lifecycle rules to bucket: {bucket_name}")
        
        # Show the updated configuration
        print("\nUpdated lifecycle rules:")
        new_rules = get_current_rules(s3, bucket_name)
        print_lifecycle_rules(new_rules)
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            print(f"Error: Bucket {bucket_name} does not exist")
        elif error_code == '403':
            print(f"Error: Permission denied for bucket {bucket_name}")
        else:
            print(f"Error applying lifecycle rules: {str(e)}")
        sys.exit(1)

def main() -> None:
    parser = argparse.ArgumentParser(description='Apply S3 lifecycle rules to a bucket')
    parser.add_argument('config_file', help='Path to lifecycle configuration JSON file')
    parser.add_argument('bucket_name', help='Name of the S3 bucket')
    
    args = parser.parse_args()
    
    # Load the configuration
    lifecycle_config = load_lifecycle_config(args.config_file)
    
    # Apply the rules
    apply_lifecycle_rules(args.bucket_name, lifecycle_config)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import os
import re
import json
import argparse
import boto3
import logging
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - SecurityReview - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("security_review.log"),
        logging.StreamHandler()
    ]
)

class SecurityReviewer:
    def __init__(self, codebase_dir='.', aws_region='us-east-1'):
        """Initialize security reviewer"""
        self.codebase_dir = codebase_dir
        self.aws_region = aws_region
        
        # File patterns to scan
        self.code_files = []
        
        # AWS Security
        self.aws_client = boto3.client('iam', region_name=aws_region)
        
        # Results
        self.issues = []
        
        logging.info(f"Security review initialized for codebase in {codebase_dir}")

    def find_python_files(self):
        """Find all Python files in the codebase"""
        python_files = []
        for root, _, files in os.walk(self.codebase_dir):
            for file in files:
                if file.endswith('.py'):
                    python_files.append(os.path.join(root, file))
        
        self.code_files = python_files
        logging.info(f"Found {len(python_files)} Python files to scan")
        return python_files

    def check_hard_coded_credentials(self):
        """Check for hard-coded credentials in code files"""
        logging.info("Checking for hard-coded credentials...")
        credential_patterns = [
            r'aws_access_key_id\s*=\s*["\'][\w]+["\']',
            r'aws_secret_access_key\s*=\s*["\']\S+["\']',
            r'password\s*=\s*["\']\S+["\']',
            r'secret\s*=\s*["\']\S+["\']',
            r'apikey\s*=\s*["\']\S+["\']',
            r'api_key\s*=\s*["\']\S+["\']'
        ]
        
        issues_found = 0
        for file_path in self.code_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    
                for pattern in credential_patterns:
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        # Check if it's in a comment
                        line_start = content.rfind('\n', 0, match.start()) + 1
                        line = content[line_start:content.find('\n', match.start())]
                        if not line.strip().startswith('#'):
                            self.issues.append({
                                'type': 'Hard-coded Credential',
                                'severity': 'HIGH',
                                'file': file_path,
                                'description': f"Potential hard-coded credential found: {match.group(0)}"
                            })
                            issues_found += 1
            except Exception as e:
                logging.error(f"Error scanning {file_path}: {e}")
        
        logging.info(f"Found {issues_found} hard-coded credential issues")
        return issues_found

    def check_missing_input_validation(self):
        """Check for missing input validation in web endpoints"""
        logging.info("Checking for missing input validation...")
        # Look for Flask routes without validation
        route_pattern = r'@app\.route\(["\'](.+?)["\']'
        validation_pattern = r'request\.(json|args|form).get\(["\'](\w+)["\']\)'
        
        issues_found = 0
        for file_path in self.code_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Find all route definitions
                routes = re.finditer(route_pattern, content)
                for route_match in routes:
                    route = route_match.group(1)
                    
                    # Find the function associated with this route
                    func_start = content.find('def', route_match.end())
                    if func_start == -1:
                        continue
                    
                    func_end = content.find('\n\n', func_start)
                    if func_end == -1:
                        func_end = len(content)
                    
                    func_content = content[func_start:func_end]
                    
                    # Check if the function uses request data
                    if 'request' in func_content:
                        # Check if there's proper validation
                        params = re.finditer(validation_pattern, func_content)
                        param_list = [p.group(2) for p in params]
                        
                        if not param_list or 'request.json' in func_content and not any(validation for validation in ['if not request.json:', 'if request.json is None:']):
                            self.issues.append({
                                'type': 'Missing Input Validation',
                                'severity': 'MEDIUM',
                                'file': file_path,
                                'description': f"Route {route} may be missing input validation"
                            })
                            issues_found += 1
            except Exception as e:
                logging.error(f"Error scanning {file_path} for input validation: {e}")
        
        logging.info(f"Found {issues_found} potential missing input validation issues")
        return issues_found

    def check_error_handling(self):
        """Check for proper error handling"""
        logging.info("Checking for proper error handling...")
        # Look for try blocks without specific exception handling
        try_pattern = r'try:'
        except_pattern = r'except\s+(\w+|\(\w+(?:,\s*\w+)*\)):'
        bare_except_pattern = r'except:'
        
        issues_found = 0
        for file_path in self.code_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Find all try blocks
                try_positions = [m.start() for m in re.finditer(try_pattern, content)]
                
                for pos in try_positions:
                    # Find the next 'except' or 'finally'
                    block_end = content.find('\n\n', pos)
                    if block_end == -1:
                        block_end = len(content)
                    
                    block = content[pos:block_end]
                    
                    # Check if the block has except statements
                    specific_excepts = re.search(except_pattern, block)
                    bare_except = re.search(bare_except_pattern, block)
                    
                    if not specific_excepts and bare_except:
                        self.issues.append({
                            'type': 'Broad Exception Handling',
                            'severity': 'LOW',
                            'file': file_path,
                            'description': f"Using bare 'except:' without specifying exception type"
                        })
                        issues_found += 1
            except Exception as e:
                logging.error(f"Error scanning {file_path} for error handling: {e}")
        
        logging.info(f"Found {issues_found} error handling issues")
        return issues_found

    def check_aws_security(self):
        """Check AWS security configuration"""
        logging.info("Checking AWS security configuration...")
        try:
            # Check if AWS credentials are available
            session = boto3.Session(region_name=self.aws_region)
            sts = session.client('sts')
            
            try:
                # Check identity
                identity = sts.get_caller_identity()
                logging.info(f"AWS Identity: {identity['Arn']}")
                
                # Check S3 bucket policies
                s3 = session.client('s3')
                try:
                    buckets = s3.list_buckets()
                    for bucket in buckets['Buckets']:
                        bucket_name = bucket['Name']
                        
                        # Check if bucket matches crawler pattern
                        if 'crawler' in bucket_name.lower() or 'crawl' in bucket_name.lower():
                            try:
                                # Check bucket policy
                                policy = s3.get_bucket_policy(Bucket=bucket_name)
                                
                                # Check for public access
                                if '"Principal":"*"' in policy['Policy'] or '"Principal": "*"' in policy['Policy']:
                                    self.issues.append({
                                        'type': 'S3 Security',
                                        'severity': 'HIGH',
                                        'resource': bucket_name,
                                        'description': f"S3 bucket {bucket_name} may allow public access"
                                    })
                            except s3.exceptions.ClientError:
                                # No policy is actually okay
                                pass
                            
                            # Check bucket ACL
                            try:
                                acl = s3.get_bucket_acl(Bucket=bucket_name)
                                for grant in acl.get('Grants', []):
                                    grantee = grant.get('Grantee', {})
                                    if 'URI' in grantee and 'AllUsers' in grantee['URI']:
                                        self.issues.append({
                                            'type': 'S3 Security',
                                            'severity': 'HIGH',
                                            'resource': bucket_name,
                                            'description': f"S3 bucket {bucket_name} has public access in ACL"
                                        })
                            except Exception as e:
                                logging.error(f"Error checking ACL for bucket {bucket_name}: {e}")
                                
                except Exception as e:
                    logging.error(f"Error listing S3 buckets: {e}")
                    self.issues.append({
                        'type': 'AWS Access',
                        'severity': 'MEDIUM',
                        'resource': 'S3',
                        'description': f"Unable to check S3 buckets: {e}"
                    })
                
                # Check SQS queue permissions
                sqs = session.client('sqs')
                try:
                    queues = sqs.list_queues(QueueNamePrefix='crawler')
                    for queue_url in queues.get('QueueUrls', []):
                        try:
                            # Get queue attributes
                            attrs = sqs.get_queue_attributes(
                                QueueUrl=queue_url,
                                AttributeNames=['Policy']
                            )
                            
                            if 'Policy' in attrs['Attributes']:
                                policy = json.loads(attrs['Attributes']['Policy'])
                                for statement in policy.get('Statement', []):
                                    principal = statement.get('Principal', {})
                                    if principal == "*" or principal.get('AWS') == "*":
                                        self.issues.append({
                                            'type': 'SQS Security',
                                            'severity': 'HIGH',
                                            'resource': queue_url,
                                            'description': f"SQS queue has public access in policy"
                                        })
                        except Exception as e:
                            logging.error(f"Error checking policy for queue {queue_url}: {e}")
                except Exception as e:
                    logging.error(f"Error listing SQS queues: {e}")
                    self.issues.append({
                        'type': 'AWS Access',
                        'severity': 'MEDIUM',
                        'resource': 'SQS',
                        'description': f"Unable to check SQS queues: {e}"
                    })
            except Exception as e:
                logging.error(f"Error getting caller identity: {e}")
                self.issues.append({
                    'type': 'AWS Authentication',
                    'severity': 'HIGH',
                    'resource': 'AWS Account',
                    'description': f"Failed to authenticate with AWS: {e}"
                })
        except Exception as e:
            logging.error(f"Error setting up AWS session: {e}")
            self.issues.append({
                'type': 'AWS Configuration',
                'severity': 'MEDIUM',
                'resource': 'AWS',
                'description': f"AWS SDK configuration error: {e}"
            })
        
        aws_issues = [i for i in self.issues if i['type'].startswith('AWS') or i['type'].endswith('Security')]
        logging.info(f"Found {len(aws_issues)} AWS security issues")
        return len(aws_issues)

    def check_network_security(self):
        """Check for network security issues"""
        logging.info("Checking network security configuration...")
        # Look for insecure requests or connections
        insecure_patterns = [
            (r'http://(?!localhost)', 'Insecure HTTP protocol usage'),
            (r'verify\s*=\s*False', 'SSL certificate verification disabled'),
            (r'allow_redirects\s*=\s*True', 'Unrestricted HTTP redirects'),
            (r'timeout\s*=\s*None', 'No request timeout specified')
        ]
        
        issues_found = 0
        for file_path in self.code_files:
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                for pattern, description in insecure_patterns:
                    matches = re.finditer(pattern, content)
                    for match in matches:
                        line_start = content.rfind('\n', 0, match.start()) + 1
                        line = content[line_start:content.find('\n', match.start())]
                        if not line.strip().startswith('#'):
                            self.issues.append({
                                'type': 'Network Security',
                                'severity': 'MEDIUM',
                                'file': file_path,
                                'description': f"{description}: {match.group(0)}"
                            })
                            issues_found += 1
            except Exception as e:
                logging.error(f"Error scanning {file_path} for network security: {e}")
        
        logging.info(f"Found {issues_found} network security issues")
        return issues_found

    def run_review(self):
        """Run the security review"""
        logging.info("Starting security review...")
        
        # Find Python files to scan
        self.find_python_files()
        
        # Run checks
        self.check_hard_coded_credentials()
        self.check_missing_input_validation()
        self.check_error_handling()
        self.check_network_security()
        
        # Check AWS security if requested
        try:
            self.check_aws_security()
        except Exception as e:
            logging.error(f"Error during AWS security check: {e}")
        
        # Generate report
        self.generate_report()
        
        return self.issues

    def generate_report(self):
        """Generate a security report"""
        logging.info("Generating security report...")
        
        # Group issues by severity
        high_issues = [i for i in self.issues if i['severity'] == 'HIGH']
        medium_issues = [i for i in self.issues if i['severity'] == 'MEDIUM']
        low_issues = [i for i in self.issues if i['severity'] == 'LOW']
        
        print("\n===== SECURITY REVIEW REPORT =====")
        print(f"Total issues: {len(self.issues)}")
        print(f"HIGH severity: {len(high_issues)}")
        print(f"MEDIUM severity: {len(medium_issues)}")
        print(f"LOW severity: {len(low_issues)}")
        
        if high_issues:
            print("\n----- HIGH SEVERITY ISSUES -----")
            for issue in high_issues:
                self._print_issue(issue)
        
        if medium_issues:
            print("\n----- MEDIUM SEVERITY ISSUES -----")
            for issue in medium_issues:
                self._print_issue(issue)
                
        if low_issues:
            print("\n----- LOW SEVERITY ISSUES -----")
            for issue in low_issues:
                self._print_issue(issue)
        
        # Save report to file
        try:
            with open('security_review_report.json', 'w') as f:
                json.dump({
                    'summary': {
                        'total': len(self.issues),
                        'high': len(high_issues),
                        'medium': len(medium_issues),
                        'low': len(low_issues)
                    },
                    'issues': self.issues
                }, f, indent=2)
                
            logging.info("Report saved to security_review_report.json")
        except Exception as e:
            logging.error(f"Error saving report: {e}")
    
    def _print_issue(self, issue):
        """Print a single issue"""
        print(f"\n[{issue['type']}] {issue['description']}")
        if 'file' in issue:
            print(f"Location: {issue['file']}")
        elif 'resource' in issue:
            print(f"Resource: {issue['resource']}")

def main():
    parser = argparse.ArgumentParser(description='Security Review for Distributed Web Crawler')
    parser.add_argument('--dir', default='.', help='Codebase directory to scan')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--skip-aws', action='store_true', help='Skip AWS security checks')
    
    args = parser.parse_args()
    
    reviewer = SecurityReviewer(codebase_dir=args.dir, aws_region=args.region)
    issues = reviewer.run_review()
    
    return len(issues) == 0  # Return success if no issues found

if __name__ == "__main__":
    main() 
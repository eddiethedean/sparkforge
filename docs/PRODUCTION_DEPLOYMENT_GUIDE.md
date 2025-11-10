# SparkForge Production Deployment Guide

This guide provides comprehensive instructions for deploying SparkForge in various production environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Local Development Deployment](#local-development-deployment)
4. [Cloud Deployment](#cloud-deployment)
5. [Container Deployment](#container-deployment)
6. [Kubernetes Deployment](#kubernetes-deployment)
7. [CI/CD Integration](#cicd-integration)
8. [Monitoring and Logging](#monitoring-and-logging)
9. [Security Configuration](#security-configuration)
10. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

- **Python**: 3.8 or higher
- **Java**: 11 or higher (required for PySpark)
- **Memory**: Minimum 8GB RAM (16GB+ recommended for production)
- **Storage**: Minimum 50GB free space
- **Network**: Access to data sources and external services

### Software Dependencies

```bash
# Core dependencies
pip install pyspark>=3.5.0,<3.6.0
pip install pipeline_builder

# Optional dependencies for enhanced functionality
pip install pandas numpy matplotlib seaborn
pip install jupyter notebook
pip install pytest pytest-cov
```

## Environment Setup

### 1. Python Environment

Create a dedicated Python environment:

```bash
# Create virtual environment
python -m venv pipeline_builder-env

# Activate environment
source pipeline_builder-env/bin/activate  # Linux/Mac
# or
pipeline_builder-env\Scripts\activate     # Windows

# Install SparkForge
pip install pipeline_builder
```

### 2. Spark Configuration

Configure Spark for your environment:

```python
from pyspark.sql import SparkSession
import os

# Production Spark configuration
spark_config = {
    "spark.app.name": "SparkForge-Production",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.driver.memory": "4g",
    "spark.driver.maxResultSize": "2g",
    "spark.executor.memory": "8g",
    "spark.executor.cores": "4",
    "spark.executor.instances": "4"
}

# Create Spark session
spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", "/path/to/warehouse") \
    .config(conf=spark_config) \
    .getOrCreate()
```

### 3. Environment Variables

Set up environment variables:

```bash
# Production environment variables
export SPARKFORGE_ENV=production
export SPARKFORGE_LOG_LEVEL=INFO
export SPARKFORGE_DATA_PATH=/data/pipeline_builder
export SPARKFORGE_CONFIG_PATH=/config/pipeline_builder
export SPARKFORGE_LOG_PATH=/logs/pipeline_builder

# Database connections
export SPARKFORGE_DATABASE_URL=postgresql://user:pass@host:port/db
export SPARKFORGE_REDIS_URL=redis://host:port

# Security
export SPARKFORGE_SECRET_KEY=your-secret-key
export SPARKFORGE_JWT_SECRET=your-jwt-secret
```

## Local Development Deployment

### 1. Quick Start

```bash
# Clone repository
git clone https://github.com/eddiethedean/pipeline_builder.git
cd pipeline_builder

# Setup environment
./setup.sh

# Run tests
python -m pytest tests/ -v

# Start development server
python examples/quick_start.py
```

### 2. Development Configuration

```python
# development_config.py
from pipeline_builder.models import PipelineConfig, ValidationThresholds, ParallelConfig

# Development configuration
dev_config = PipelineConfig(
    schema="dev_analytics",
    quality_thresholds=ValidationThresholds(
        bronze=70.0,  # Lower thresholds for development
        silver=75.0,
        gold=80.0
    ),
    parallel=ParallelConfig(
        enabled=True,
        max_workers=2  # Limited workers for development
    ),
    performance_monitoring=True,
    debug_mode=True
)
```

## Cloud Deployment

### 1. AWS EMR Deployment

#### EMR Cluster Configuration

```json
{
  "Name": "SparkForge-Production",
  "ReleaseLabel": "emr-6.15.0",
  "Applications": [
    {
      "Name": "Spark"
    },
    {
      "Name": "Hadoop"
    }
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Workers",
        "InstanceRole": "CORE",
        "InstanceType": "m5.2xlarge",
        "InstanceCount": 3
      }
    ],
    "Ec2KeyName": "your-key-pair",
    "KeepJobFlowAliveWhenNoSteps": true
  },
  "Configurations": [
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
      }
    }
  ],
  "BootstrapActions": [
    {
      "Name": "Install SparkForge",
      "ScriptBootstrapAction": {
        "Path": "s3://your-bucket/bootstrap/install-pipeline_builder.sh"
      }
    }
  ]
}
```

#### Bootstrap Script

```bash
#!/bin/bash
# install-pipeline_builder.sh

# Update system
sudo yum update -y

# Install Python dependencies
sudo pip3 install --upgrade pip
sudo pip3 install pipeline_builder

# Create application directory
sudo mkdir -p /opt/pipeline_builder
sudo chown hadoop:hadoop /opt/pipeline_builder

# Download application code
aws s3 cp s3://your-bucket/pipeline_builder-app/ /opt/pipeline_builder/ --recursive

# Set permissions
chmod +x /opt/pipeline_builder/*.py
```

### 2. Azure Databricks Deployment

#### Databricks Cluster Configuration

```json
{
  "cluster_name": "pipeline_builder-production",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "driver_node_type_id": "Standard_DS3_v2",
  "num_workers": 4,
  "autotermination_minutes": 60,
  "enable_elastic_disk": true,
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
  },
  "custom_tags": {
    "Environment": "Production",
    "Application": "SparkForge"
  }
}
```

#### Databricks Job Configuration

```json
{
  "name": "SparkForge Pipeline",
  "new_cluster": {
    "cluster_name": "pipeline_builder-job-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 4
  },
  "libraries": [
    {
      "git": {
        "branch": "main",
        "git_provider": "gitHub",
        "git_url": "https://github.com/eddiethedean/sparkforge.git"
      }
    }
  ],
  "notebook_task": {
    "notebook_path": "/Shared/pipeline_builder_pipeline",
    "base_parameters": {
      "environment": "production",
      "schema": "analytics"
    }
  },
  "timeout_seconds": 3600,
  "max_retries": 2
}
```

### 3. Google Cloud Dataproc Deployment

#### Dataproc Cluster Configuration

```bash
# Create Dataproc cluster
gcloud dataproc clusters create pipeline_builder-production \
    --region=us-central1 \
    --zone=us-central1-a \
    --master-machine-type=n1-standard-4 \
    --master-boot-disk-size=100GB \
    --num-workers=4 \
    --worker-machine-type=n1-standard-4 \
    --worker-boot-disk-size=100GB \
    --image-version=2.0 \
    --properties=spark:spark.sql.adaptive.enabled=true \
    --properties=spark:spark.sql.adaptive.coalescePartitions.enabled=true \
    --initialization-actions=gs://your-bucket/init-scripts/install-pipeline_builder.sh
```

#### Initialization Script

```bash
#!/bin/bash
# install-pipeline_builder.sh

# Install SparkForge from repository
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge
pip3 install -e .

# Create application directory
mkdir -p /opt/pipeline_builder
gsutil cp -r gs://your-bucket/pipeline_builder-app/* /opt/pipeline_builder/

# Set permissions
chmod +x /opt/pipeline_builder/*.py
```

## Container Deployment

### 1. Docker Deployment

#### Dockerfile

```dockerfile
FROM openjdk:11-jre-slim

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Create application user
RUN useradd -m -u 1000 pipeline_builder

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set permissions
RUN chown -R pipeline_builder:pipeline_builder /app
USER pipeline_builder

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import pipeline_builder; print('OK')" || exit 1

# Start application
CMD ["python3", "main.py"]
```

#### Docker Compose

```yaml
version: '3.8'

services:
  pipeline_builder:
    build: .
    ports:
      - "8080:8080"
    environment:
      - SPARKFORGE_ENV=production
      - SPARKFORGE_LOG_LEVEL=INFO
      - SPARKFORGE_DATA_PATH=/data
    volumes:
      - ./data:/data
      - ./logs:/logs
      - ./config:/config
    depends_on:
      - postgres
      - redis
    restart: unless-stopped

  postgres:
    image: postgres:13
    environment:
      - POSTGRES_DB=pipeline_builder
      - POSTGRES_USER=pipeline_builder
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:6-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - pipeline_builder

volumes:
  postgres_data:
  redis_data:
```

### 2. Docker Build and Deploy

```bash
# Build Docker image
docker build -t pipeline_builder:latest .

# Run container
docker run -d \
    --name pipeline_builder-production \
    -p 8080:8080 \
    -e SPARKFORGE_ENV=production \
    -v $(pwd)/data:/data \
    -v $(pwd)/logs:/logs \
    pipeline_builder:latest

# Check container status
docker ps
docker logs pipeline_builder-production
```

## Kubernetes Deployment

### 1. Kubernetes Manifests

#### Namespace

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: pipeline_builder
  labels:
    name: pipeline_builder
```

#### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pipeline_builder-config
  namespace: pipeline_builder
data:
  config.yaml: |
    environment: production
    log_level: INFO
    data_path: /data
    config_path: /config
    log_path: /logs
  spark.conf: |
    spark.sql.adaptive.enabled=true
    spark.sql.adaptive.coalescePartitions.enabled=true
    spark.serializer=org.apache.spark.serializer.KryoSerializer
```

#### Secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: pipeline_builder-secrets
  namespace: pipeline_builder
type: Opaque
data:
  database-url: cG9zdGdyZXNxbDovL3VzZXI6cGFzc0Bob3N0OnBvcnQvZGI=
  redis-url: cmVkaXM6Ly9ob3N0OnBvcnQ=
  secret-key: eW91ci1zZWNyZXQta2V5
  jwt-secret: eW91ci1qd3Qtc2VjcmV0
```

#### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline_builder
  namespace: pipeline_builder
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pipeline_builder
  template:
    metadata:
      labels:
        app: pipeline_builder
    spec:
      containers:
      - name: pipeline_builder
        image: pipeline_builder:latest
        ports:
        - containerPort: 8080
        env:
        - name: SPARKFORGE_ENV
          valueFrom:
            configMapKeyRef:
              name: pipeline_builder-config
              key: environment
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: pipeline_builder-secrets
              key: database-url
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: pipeline_builder-secrets
              key: redis-url
        volumeMounts:
        - name: config-volume
          mountPath: /config
        - name: data-volume
          mountPath: /data
        - name: logs-volume
          mountPath: /logs
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config-volume
        configMap:
          name: pipeline_builder-config
      - name: data-volume
        persistentVolumeClaim:
          claimName: pipeline_builder-data-pvc
      - name: logs-volume
        persistentVolumeClaim:
          claimName: pipeline_builder-logs-pvc
```

#### Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: pipeline_builder-service
  namespace: pipeline_builder
spec:
  selector:
    app: pipeline_builder
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8080
  type: LoadBalancer
```

#### Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: pipeline_builder-ingress
  namespace: pipeline_builder
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  tls:
  - hosts:
    - pipeline_builder.yourdomain.com
    secretName: pipeline_builder-tls
  rules:
  - host: pipeline_builder.yourdomain.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: pipeline_builder-service
            port:
              number: 80
```

### 2. Deploy to Kubernetes

```bash
# Apply manifests
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml

# Check deployment status
kubectl get pods -n pipeline_builder
kubectl get services -n pipeline_builder
kubectl get ingress -n pipeline_builder

# View logs
kubectl logs -f deployment/pipeline_builder -n pipeline_builder
```

## CI/CD Integration

### 1. GitHub Actions Deployment

```yaml
name: Deploy to Production

on:
  push:
    branches: [main]
    tags: ['v*']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Build Docker image
      run: |
        docker build -t pipeline_builder:${{ github.sha }} .
        docker tag pipeline_builder:${{ github.sha }} pipeline_builder:latest
    
    - name: Push to registry
      run: |
        echo ${{ secrets.DOCKER_PASSWORD }} | docker login -u ${{ secrets.DOCKER_USERNAME }} --password-stdin
        docker push pipeline_builder:${{ github.sha }}
        docker push pipeline_builder:latest
    
    - name: Deploy to Kubernetes
      run: |
        echo "${{ secrets.KUBE_CONFIG }}" | base64 -d > kubeconfig
        export KUBECONFIG=kubeconfig
        kubectl set image deployment/pipeline_builder pipeline_builder=pipeline_builder:${{ github.sha }} -n pipeline_builder
        kubectl rollout status deployment/pipeline_builder -n pipeline_builder
```

### 2. Automated Testing

```yaml
name: Production Testing

on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target environment'
        required: true
        default: 'staging'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Run integration tests
      run: |
        python -m pytest tests/integration/ -v --tb=short
    
    - name: Run system tests
      run: |
        python -m pytest tests/system/ -v --tb=short
    
    - name: Performance tests
      run: |
        python -m pytest tests/performance/ -v --tb=short
```

## Monitoring and Logging

### 1. Application Monitoring

```python
# monitoring.py
import logging
import time
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/pipeline_builder.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('pipeline_builder')

def monitor_performance(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} executed successfully in {execution_time:.2f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f}s: {str(e)}")
            raise
    return wrapper
```

### 2. Health Checks

```python
# health_check.py
from flask import Flask, jsonify
import psutil
import os

app = Flask(__name__)

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Check system resources
        cpu_percent = psutil.cpu_percent()
        memory_percent = psutil.virtual_memory().percent
        disk_percent = psutil.disk_usage('/').percent
        
        # Check application status
        app_status = "healthy" if cpu_percent < 90 and memory_percent < 90 else "degraded"
        
        return jsonify({
            "status": app_status,
            "cpu_percent": cpu_percent,
            "memory_percent": memory_percent,
            "disk_percent": disk_percent,
            "timestamp": time.time()
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

@app.route('/ready')
def readiness_check():
    """Readiness check endpoint"""
    try:
        # Check if application is ready to serve requests
        # Add your readiness checks here
        return jsonify({"status": "ready"})
    except Exception as e:
        return jsonify({"status": "not_ready", "error": str(e)}), 503
```

### 3. Log Aggregation

```yaml
# fluentd.conf
<source>
  @type tail
  path /logs/pipeline_builder.log
  pos_file /var/log/fluentd/pipeline_builder.log.pos
  tag pipeline_builder
  format json
</source>

<match pipeline_builder>
  @type elasticsearch
  host elasticsearch.logging.svc.cluster.local
  port 9200
  index_name pipeline_builder
  type_name _doc
</match>
```

## Security Configuration

### 1. Authentication and Authorization

```python
# auth.py
import jwt
from functools import wraps
from flask import request, jsonify

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token:
            return jsonify({'message': 'Token is missing'}), 401
        
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data['user_id']
        except:
            return jsonify({'message': 'Token is invalid'}), 401
        
        return f(current_user, *args, **kwargs)
    return decorated
```

### 2. Data Encryption

```python
# encryption.py
from cryptography.fernet import Fernet
import base64

class DataEncryption:
    def __init__(self, key):
        self.cipher = Fernet(key)
    
    def encrypt(self, data):
        """Encrypt sensitive data"""
        return self.cipher.encrypt(data.encode())
    
    def decrypt(self, encrypted_data):
        """Decrypt sensitive data"""
        return self.cipher.decrypt(encrypted_data).decode()
```

### 3. Network Security

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: pipeline_builder-network-policy
  namespace: pipeline_builder
spec:
  podSelector:
    matchLabels:
      app: pipeline_builder
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: nginx
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: postgres
    ports:
    - protocol: TCP
      port: 5432
```

## Troubleshooting

### 1. Common Deployment Issues

#### Memory Issues
```bash
# Check memory usage
kubectl top pods -n pipeline_builder

# Adjust memory limits
kubectl patch deployment pipeline_builder -n pipeline_builder -p '{"spec":{"template":{"spec":{"containers":[{"name":"pipeline_builder","resources":{"limits":{"memory":"8Gi"}}}]}}}}'
```

#### Pod Crash Issues
```bash
# Check pod logs
kubectl logs -f deployment/pipeline_builder -n pipeline_builder

# Check pod events
kubectl describe pod <pod-name> -n pipeline_builder
```

#### Database Connection Issues
```bash
# Test database connectivity
kubectl exec -it deployment/pipeline_builder -n pipeline_builder -- python -c "import psycopg2; psycopg2.connect('$DATABASE_URL')"
```

### 2. Performance Issues

```bash
# Check resource usage
kubectl top nodes
kubectl top pods -n pipeline_builder

# Scale deployment
kubectl scale deployment pipeline_builder --replicas=5 -n pipeline_builder
```

### 3. Log Analysis

```bash
# View recent logs
kubectl logs --since=1h deployment/pipeline_builder -n pipeline_builder

# Search for errors
kubectl logs deployment/pipeline_builder -n pipeline_builder | grep ERROR

# Export logs
kubectl logs deployment/pipeline_builder -n pipeline_builder > pipeline_builder.log
```

## Conclusion

This guide provides comprehensive instructions for deploying SparkForge in various production environments. Key points to remember:

1. **Environment Setup**: Properly configure Python, Spark, and system dependencies
2. **Resource Planning**: Allocate appropriate CPU, memory, and storage resources
3. **Security**: Implement authentication, authorization, and data encryption
4. **Monitoring**: Set up comprehensive logging and monitoring
5. **CI/CD**: Automate deployment and testing processes
6. **Troubleshooting**: Have proper debugging and recovery procedures

For additional support, refer to the troubleshooting guide or contact the development team.

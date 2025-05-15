#!/usr/bin/env python3
"""
Prefect deployment script for the enhanced variant annotation system.
This script sets up flows, work pools, and deployments for distributed execution.
"""

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from prefect.infrastructure.docker import DockerContainer
from prefect.infrastructure.process import Process
from datetime import timedelta
import os

from summary import variant_annotation_flow, AnnotationConfig, AnnotationSource


def create_deployments():
    """Create Prefect deployments for different annotation workflows."""
    
    # Deployment 1: High-priority clinical variants (fast processing)
    clinical_deployment = Deployment.build_from_flow(
        flow=variant_annotation_flow,
        name="clinical-variant-annotation",
        parameters={
            "annotation_config": AnnotationConfig(
                enabled_sources=[AnnotationSource.CLINVAR, AnnotationSource.OPENCRAVAT],
                fallback_enabled=True,
                parallel_execution=True,
                timeout_seconds=180
            ),
            "genome_build": "hg38",
            "annotators": ["clinvar", "dbnsfp", "gnomad"]
        },
        work_pool_name="clinical-pool",
        tags=["clinical", "high-priority", "fast"],
        description="Fast annotation for clinical variants using ClinVar and OpenCravat"
    )
    
    # Deployment 2: Research variants (comprehensive annotation)
    research_deployment = Deployment.build_from_flow(
        flow=variant_annotation_flow,
        name="research-variant-annotation", 
        parameters={
            "annotation_config": AnnotationConfig(
                enabled_sources=[
                    AnnotationSource.OPENCRAVAT, 
                    AnnotationSource.ENSEMBL,
                    AnnotationSource.CLINVAR
                ],
                fallback_enabled=True,
                parallel_execution=True,
                timeout_seconds=600
            ),
            "genome_build": "hg38",
            "annotators": ["clinvar", "dbnsfp", "gnomad", "cosmic", "pfam", "interpro"]
        },
        work_pool_name="research-pool",
        tags=["research", "comprehensive", "detailed"],
        description="Comprehensive annotation for research variants using all sources"
    )
    
    # Deployment 3: Batch processing (optimized for throughput)
    batch_deployment = Deployment.build_from_flow(
        flow=variant_annotation_flow,
        name="batch-variant-annotation",
        parameters={
            "annotation_config": AnnotationConfig(
                enabled_sources=[AnnotationSource.OPENCRAVAT],
                fallback_enabled=False,
                parallel_execution=True,
                timeout_seconds=120
            ),
            "genome_build": "hg38", 
            "annotators": ["clinvar", "dbnsfp", "gnomad"]
        },
        work_pool_name="batch-pool",
        tags=["batch", "high-throughput", "opencravat-only"],
        description="Optimized annotation for batch processing using OpenCravat only"
    )
    
    return [clinical_deployment, research_deployment, batch_deployment]


def setup_work_pools():
    """
    Setup instructions for Prefect work pools.
    Run these commands manually in your terminal.
    """
    
    commands = [
        # Create work pools for different types of workloads
        "prefect work-pool create clinical-pool --type process",
        "prefect work-pool create research-pool --type process", 
        "prefect work-pool create batch-pool --type process",
        
        # For Docker-based execution (optional)
        "prefect work-pool create docker-clinical-pool --type docker",
        "prefect work-pool create docker-research-pool --type docker",
        
        # Start workers for each pool
        "prefect worker start --pool clinical-pool --name clinical-worker-1 &",
        "prefect worker start --pool research-pool --name research-worker-1 &", 
        "prefect worker start --pool batch-pool --name batch-worker-1 &",
    ]
    
    print("=== Work Pool Setup Commands ===")
    print("Run these commands in separate terminals:\n")
    for cmd in commands:
        print(f"  {cmd}")
    print()


def deploy_flows():
    """Deploy all annotation flows to Prefect."""
    
    deployments = create_deployments()
    
    print("=== Deploying Variant Annotation Flows ===\n")
    
    for deployment in deployments:
        try:
            deployment_id = deployment.apply()
            print(f"✅ Deployed: {deployment.name}")
            print(f"   ID: {deployment_id}")
            print(f"   Work Pool: {deployment.work_pool_name}")
            print(f"   Tags: {deployment.tags}")
            print()
        except Exception as e:
            print(f"❌ Failed to deploy {deployment.name}: {e}")
            print()


# Docker configuration for containerized deployment
def create_docker_deployment():
    """Create a Docker-based deployment for containerized execution."""
    
    docker_container = DockerContainer(
        image="python:3.11-slim",
        image_pull_policy="Always",
        commands=[
            "pip install -r requirements.txt",
            "pip install open-cravat",  # Install OpenCravat
        ],
        env={
            "PREFECT_API_URL": os.getenv("PREFECT_API_URL", "http://localhost:4200/api"),
            "OPENCRAVAT_DEFAULT_ASSEMBLY": "hg38"
        }
    )
    
    docker_deployment = Deployment.build_from_flow(
        flow=variant_annotation_flow,
        name="docker-variant-annotation",
        infrastructure=docker_container,
        work_pool_name="docker-clinical-pool",
        tags=["docker", "containerized"],
        description="Containerized variant annotation using Docker"
    )
    
    return docker_deployment


def main():
    """Main deployment function."""
    
    print("🧬 Variant Annotation System - Prefect Deployment")
    print("=" * 50)
    print()
    
    # Check if Prefect server is running
    try:
        from prefect.client.orchestration import get_client
        client = get_client()
        print("✅ Prefect server connection successful")
    except Exception as e:
        print("❌ Cannot connect to Prefect server")
        print("   Please start the server with: prefect server start")
        print(f"   Error: {e}")
        return
    
    print()
    
    # Setup work pools
    setup_work_pools()
    
    # Deploy flows
    deploy_flows()
    
    # Optional: Deploy Docker version
    docker_deploy = input("Deploy Docker version? (y/n): ").lower().strip()
    if docker_deploy == 'y':
        try:
            docker_deployment = create_docker_deployment()
            docker_deployment.apply()
            print("✅ Docker deployment created successfully")
        except Exception as e:
            print(f"❌ Docker deployment failed: {e}")
    
    print("\n=== Deployment Complete ===")
    print("Next steps:")
    print("1. Start workers using the commands shown above")
    print("2. Visit Prefect UI: http://localhost:4200")
    print("3. Trigger flows manually or via API calls")
    print("4. Monitor execution in the Prefect dashboard")


if __name__ == "__main__":
    main() 
from prefect.deployments import Deployment
from prefect.infrastructure import Process
from prefect.server.schemas.schedules import RRuleSchedule
from flows import async_enrichment_process
import os

def setup_prefect_deployment():
    """Set up the Prefect deployment during service startup"""
    
    # Create a work queue for our enrichment tasks
    WORK_QUEUE_NAME = "enrichment-queue"
    
    # Build deployment
    deployment = Deployment.build_from_flow(
        flow=async_enrichment_process,
        name="enrichment-flow-deployment",
        work_queue_name=WORK_QUEUE_NAME,
        infrastructure=Process(
            working_dir=os.getcwd(),
        ),
    )
    
    deployment.apply()
    
    return WORK_QUEUE_NAME

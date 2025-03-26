from prefect import flow
from flows import async_enrichment_process
import uuid
from loguru import logger


def setup_prefect_deployment():
    """Set up the Prefect deployment during service startup"""
    
    WORK_QUEUE_NAME = "enrichment-queue"
    
    try:
        # Create a unique deployment name to avoid conflicts
        deployment_name = f"async_enrichment_process-{uuid.uuid4()}"
        
        # Deploy the flow using serve()
        deployment = async_enrichment_process.serve(
            name=deployment_name,
            work_queue_name=WORK_QUEUE_NAME,
            tags=["enrichment-process"],
            pause_on_shutdown=False
        )
        
        logger.info(f"Deployment created successfully: {deployment_name}")
        
        return WORK_QUEUE_NAME
    
    except Exception as e:
        logger.error(f"Error setting up Prefect deployment: {e}")
        return None
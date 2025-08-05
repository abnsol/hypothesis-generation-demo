import os
import pandas as pd
from prefect import task
from loguru import logger
from datetime import datetime, timezone


@task(cache_policy=None)
def save_analysis_state_task(db, user_id, project_id, state_data):
    """Save analysis state to file system"""
    try:
        db.save_analysis_state(user_id, project_id, state_data)
        logger.info(f"Saved analysis state for project {project_id}")
        return True
    except Exception as e:
        logger.info(f"Error saving analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def load_analysis_state_task(db, user_id, project_id):
    """Load analysis state from file system"""
    try:
        state = db.load_analysis_state(user_id, project_id)
        if state:
            logger.info(f"Loaded analysis state for project {project_id}")
        else:
            logger.info(f"No analysis state found for project {project_id}")
        return state
    except Exception as e:
        logger.info(f"Error loading analysis state: {str(e)}")
        raise


@task(cache_policy=None)
def create_analysis_result_task(db, user_id, project_id, combined_results, output_dir):
    """Create and save analysis results"""
    try:
        # Save to output directory
        results_file = os.path.join(output_dir, "analysis_results.csv")
        combined_results.to_csv(results_file, index=False)
        
        # Save to database
        db.save_analysis_results(user_id, project_id, combined_results.to_dict('records'))
        
        logger.info(f"Analysis results saved: {results_file}")
        return results_file
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        raise


@task(cache_policy=None)
def save_lead_variant_credible_sets_task(db, user_id, project_id, lead_variant_id, credible_sets_data, metadata):
    """Save credible sets for a single lead variant incrementally"""
    try:
        # Organize data by lead variant
        lead_variant_data = {
            "lead_variant_id": lead_variant_id,
            "credible_sets": credible_sets_data,
            "metadata": metadata,
            "saved_at": datetime.now().isoformat()
        }
        
        # Save to database immediately
        db.save_lead_variant_credible_sets(user_id, project_id, lead_variant_id, lead_variant_data)
        
        logger.info(f"Saved credible sets for lead variant {lead_variant_id}: {len(credible_sets_data)} sets")
        return True
    except Exception as e:
        logger.error(f"Error saving credible sets for lead variant {lead_variant_id}: {str(e)}")
        raise


@task(cache_policy=None)
def get_project_analysis_path_task(db, user_id, project_id):
    """Get the analysis path for a project"""
    try:
        analysis_path = db.get_project_analysis_path(user_id, project_id)
        
        # Create directory structure if it doesn't exist
        os.makedirs(analysis_path, exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "preprocessed_data"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "plink_binary"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "cojo"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "expanded_regions"), exist_ok=True)
        os.makedirs(os.path.join(analysis_path, "ld"), exist_ok=True)
        
        logger.info(f"Project analysis path: {analysis_path}")
        return analysis_path
    except Exception as e:
        logger.info(f"Error getting project analysis path: {str(e)}")
        raise


def count_gwas_records(file_path):
    """Count the number of records in a GWAS file"""
    try:
        import gzip
        
        if not os.path.exists(file_path):
            logger.warning(f"GWAS file not found: {file_path}")
            return 0
        
        count = 0
        
        # Handle gzipped files
        if file_path.endswith('.gz') or file_path.endswith('.bgz'):
            with gzip.open(file_path, 'rt') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        else:
            with open(file_path, 'r') as f:
                # Skip header
                next(f, None)
                for line in f:
                    if line.strip():  # Skip empty lines
                        count += 1
        
        return count
    except Exception as e:
        logger.warning(f"Error counting GWAS records in {file_path}: {str(e)}")
        return 0


def get_project_with_full_data(db, user_id, project_id):
    """Get comprehensive project data including state, hypotheses, and credible sets"""
    try:
        # Get basic project info
        project = db.get_projects(user_id, project_id)
        if not project:
            return {"error": "Project not found"}, 404
        
        # Get analysis state (may be None for new projects)
        analysis_state = db.load_analysis_state(user_id, project_id)
        if not analysis_state:
            analysis_state = {"status": "not_started"}
        
        # Get credible sets with simplified metadata
        credible_sets_data = []
        total_credible_sets_count = 0
        total_variants_count = 0
        analysis_parameters = {}
        
        try:
            credible_sets_raw = db.get_lead_variant_credible_sets(user_id, project_id)
            if credible_sets_raw:
                if isinstance(credible_sets_raw, list):
                    credible_sets_data = [
                        {
                            "lead_variant_id": cs["lead_variant_id"],
                            "credible_sets": cs["data"].get("credible_sets", []),
                            "metadata": {
                                # Keep only region-specific metadata
                                "chr": cs["data"].get("metadata", {}).get("chr"),
                                "position": cs["data"].get("metadata", {}).get("position"),
                                "total_variants_analyzed": cs["data"].get("metadata", {}).get("total_variants_analyzed"),
                                "credible_sets_count": cs["data"].get("metadata", {}).get("credible_sets_count"),
                                "completed_at": cs["data"].get("metadata", {}).get("completed_at")
                            },
                            "credible_sets_count": len(cs["data"].get("credible_sets", [])),
                            "total_variants_in_lead": sum(
                                len(credible_set.get("variants", [])) 
                                for credible_set in cs["data"].get("credible_sets", [])
                            )
                        }
                        for cs in credible_sets_raw
                    ]
                    
                    # Calculate total counts
                    total_credible_sets_count = sum(len(cs["data"].get("credible_sets", [])) for cs in credible_sets_raw)
                    total_variants_count = sum(
                        len(credible_set.get("variants", [])) 
                        for cs in credible_sets_raw 
                        for credible_set in cs["data"].get("credible_sets", [])
                    )
                    
                    # Extract analysis parameters from first credible set's metadata
                    if credible_sets_raw and credible_sets_raw[0]["data"].get("metadata"):
                        metadata = credible_sets_raw[0]["data"]["metadata"]
                        analysis_parameters = {
                            "population": metadata.get("population"),
                            "ref_genome": metadata.get("ref_genome"),
                            "finemap_window_kb": metadata.get("finemap_window_kb"),
                            "coverage": metadata.get("coverage"),
                            "min_abs_corr": metadata.get("min_abs_corr"),
                            "maf_threshold": metadata.get("maf_threshold"),
                            "seed": metadata.get("seed"),
                            "L": metadata.get("L")
                        }
                else:
                    # Single result
                    credible_sets_data = [{
                        "lead_variant_id": credible_sets_raw["lead_variant_id"],
                        "credible_sets": credible_sets_raw["data"].get("credible_sets", []),
                        "metadata": {
                            # Keep only region-specific metadata
                            "chr": credible_sets_raw["data"].get("metadata", {}).get("chr"),
                            "position": credible_sets_raw["data"].get("metadata", {}).get("position"),
                            "total_variants_analyzed": credible_sets_raw["data"].get("metadata", {}).get("total_variants_analyzed"),
                            "credible_sets_count": credible_sets_raw["data"].get("metadata", {}).get("credible_sets_count"),
                            "completed_at": credible_sets_raw["data"].get("metadata", {}).get("completed_at")
                        },
                        "credible_sets_count": len(credible_sets_raw["data"].get("credible_sets", [])),
                        "total_variants_in_lead": sum(
                            len(credible_set.get("variants", []))
                            for credible_set in credible_sets_raw["data"].get("credible_sets", [])
                        )
                    }]
                    
                    # Calculate total counts for single result
                    total_credible_sets_count = len(credible_sets_raw["data"].get("credible_sets", []))
                    total_variants_count = sum(
                        len(credible_set.get("variants", []))
                        for credible_set in credible_sets_raw["data"].get("credible_sets", [])
                    )
                    
                    # Extract analysis parameters
                    if credible_sets_raw["data"].get("metadata"):
                        metadata = credible_sets_raw["data"]["metadata"]
                        analysis_parameters = {
                            "population": metadata.get("population"),
                            "ref_genome": metadata.get("ref_genome"),
                            "finemap_window_kb": metadata.get("finemap_window_kb"),
                            "coverage": metadata.get("coverage"),
                            "min_abs_corr": metadata.get("min_abs_corr"),
                            "maf_threshold": metadata.get("maf_threshold"),
                            "seed": metadata.get("seed"),
                            "L": metadata.get("L")
                        }
        except Exception as cs_e:
            logger.warning(f"Could not load credible sets for project {project_id}: {cs_e}")
            credible_sets_data = []
        
        # Get hypotheses for this project
        project_hypotheses = []
        try:
            all_hypotheses = db.get_hypotheses(user_id)
            if isinstance(all_hypotheses, list):
                project_hypotheses = [
                    {
                        "id": h["id"], 
                        "variant": h.get("variant") or h.get("variant_id"),
                        "status": h.get("status", "pending"),
                        "causal_gene": h.get("causal_gene")
                    }
                    for h in all_hypotheses 
                    if h.get('project_id') == project_id
                ]
        except Exception as hyp_e:
            logger.warning(f"Could not load hypotheses for project {project_id}: {hyp_e}")
            project_hypotheses = []
        
        # Build comprehensive response
        response = {
            "id": project["id"],
            "name": project["name"],
            "phenotype": project.get("phenotype", ""),
            "gwas_file_id": project["gwas_file_id"],
            "created_at": project.get("created_at"),
            
            # Summary counts at top level (updated in real-time)
            "total_credible_sets_count": total_credible_sets_count,
            "total_variants_count": total_variants_count,
            
            # Analysis state and parameters
            "analysis_state": analysis_state,
            "analysis_parameters": analysis_parameters,
            
            # Credible sets data with simplified metadata
            "credible_sets": credible_sets_data,
            
            # Hypotheses information
            "hypotheses": project_hypotheses,
            "credible_sets": credible_sets_data
        }
        
        return response, 200
        
    except Exception as e:
        logger.error(f"Error getting comprehensive project data for {project_id}: {str(e)}")
        return {"error": f"Error retrieving project data: {str(e)}"}, 500


import subprocess
import os
import tempfile
import requests
from loguru import logger
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import time
import asyncio
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum


class AnnotationSource(Enum):
    """Enumeration of available annotation sources with priority levels."""
    OPENCRAVAT = ("opencravat", 1)
    ENSEMBL = ("ensembl", 2)
    DBSNP = ("dbsnp", 3)
    CLINVAR = ("clinvar", 4)
    
    def __init__(self, source_name: str, priority: int):
        self.source_name = source_name
        self.priority = priority


@dataclass
class AnnotationConfig:
    """Configuration for annotation sources and their priorities."""
    enabled_sources: List[AnnotationSource]
    fallback_enabled: bool = True
    parallel_execution: bool = True
    timeout_seconds: int = 300


@task(name="parse_variant", retries=2, retry_delay_seconds=1)
def parse_variant(variant_input: str) -> Tuple[str, str, str, str]:
    """Parses a variant string into chromosome, position, ref, alt, or fetches details if rsID."""
    run_logger = get_run_logger()
    try:
        if variant_input.startswith("rs"):
            return fetch_variant_from_rsid(variant_input)
        else:
            # Regular variant format (chrom:pos:ref>alt)
            chrom, pos, ref_alt = variant_input.split(":")
            ref, alt = ref_alt.split(">")
            run_logger.debug(f"Parsed variant: chrom={chrom}, pos={pos}, ref={ref}, alt={alt}")
            return chrom, pos, ref, alt
    except Exception as e:
        run_logger.error(f"Error parsing variant input '{variant_input}': {e}")
        raise


@task(name="fetch_variant_from_rsid", retries=3, retry_delay_seconds=2)
def fetch_variant_from_rsid(rsid: str) -> Tuple[str, str, str, str]:
    """Fetch variant details for an rsID from Ensembl REST API"""
    run_logger = get_run_logger()
    try:
        ensembl_rsid = rsid
        url = f"https://rest.ensembl.org/variation/human/{ensembl_rsid}?content-type=application/json"
        response = requests.get(url, timeout=30)
        response.raise_for_status()  
        
        data = response.json()
        run_logger.debug(data)
        mappings = data.get('mappings', [])
        
        if not mappings:
            raise ValueError(f"No mappings found for {rsid}")

        mapping = mappings[0]
        chrom = mapping['seq_region_name']
        chrom = f"chr{chrom}" if chrom != "MT" else "chrM"

        pos = str(mapping['start'])
        alleles = mapping['allele_string'].split('/')
        ref, alt = alleles[0], alleles[1]

        run_logger.debug(f"Fetched variant: {chrom=}, {pos=}, {ref=}, {alt=}")
        return chrom, pos, ref, alt

    except requests.exceptions.RequestException as e:
        run_logger.error(f"API request failed for {rsid}: {e}")
        raise
    except (KeyError, IndexError) as e:
        run_logger.error(f"Malformed API response for {rsid}: {e}")
        raise
    except Exception as e:
        run_logger.error(f"Unexpected error fetching {rsid}: {e}")
        raise


@task(name="create_tsv")
def create_tsv(chrom: str, pos: str, ref: str, alt: str, output_dir: str) -> str:
    """Creates a TSV file inside the given output directory."""
    run_logger = get_run_logger()
    try:
        os.makedirs(output_dir, exist_ok=True)
        tsv_file = os.path.join(output_dir, "input.tsv")
        with open(tsv_file, "w") as temp_file:
            temp_file.write("chrom\tpos\tref\talt\n")
            temp_file.write(f"{chrom}\t{pos}\t{ref}\t{alt}\n")
        run_logger.debug(f"Created TSV file at {tsv_file}")
        return tsv_file
    except Exception as e:
        run_logger.error(f"Error creating TSV file: {e}")
        raise


@task(name="generate_temp_dir")
def generate_temp_dir() -> str:
    """Generates a temporary directory."""
    run_logger = get_run_logger()
    try:
        temp_dir = tempfile.mkdtemp()
        run_logger.debug(f"Generated temporary directory at {temp_dir}")
        return temp_dir
    except Exception as e:
        run_logger.error(f"Error generating temporary directory: {e}")
        raise


@task(name="run_opencravat_annotation", retries=2, retry_delay_seconds=5)
def run_opencravat(tsv_file: str, annotators: List[str], output_dir: str, genome_build: str) -> Optional[Dict[str, Any]]:
    """
    Runs OpenCravat with the given genome build first, and if not given
    run with the default genomes.
    """
    run_logger = get_run_logger()
    
    def execute_opencravat(genome_build: str) -> Optional[Dict[str, Any]]:
        oc_command = [
            'oc', 'run', tsv_file,
            '-l', genome_build,
            '-d', output_dir,
            '-a'
        ]
        oc_command.extend(annotators)
        try:
            result = subprocess.run(oc_command, capture_output=True, text=True, check=True, timeout=120)
            if not result.stdout:
                raise Exception("No output from OpenCravat")
            run_logger.info(f"OpenCravat command output ({genome_build}): {result.stdout}")
        except subprocess.CalledProcessError as e:
            run_logger.error(f"OpenCravat command failed ({genome_build}) with error: {e.stderr}")
            return None
        except Exception as e:
            run_logger.error(f"Error running OpenCravat ({genome_build}): {e}")
            return None

        sqlite_file = None
        try:
            for file in os.listdir(output_dir):
                if file.endswith(".sqlite"):
                    sqlite_file = os.path.join(output_dir, file)
                    break
            
            if sqlite_file:
                report = convert_to_json(sqlite_file)
                return report
            else:
                run_logger.error(f"No SQLite file found in the output directory ({genome_build}).")
                return None
        except Exception as e:
            run_logger.error(f"Error processing OpenCravat output ({genome_build}): {e}")
            return None

    # First attempt with the given genome build
    report = execute_opencravat(genome_build)
    if report and any(report.get("annotations", {}).values()):
        return report
    
    next_genome = "hg38" if genome_build == "hg19" else "hg19"
    
    # if the given genome build didnt return try with a different genome build
    run_logger.info(f"No annotations found with {genome_build}. Retrying with {next_genome}.")

    return execute_opencravat(next_genome)


@task(name="run_ensembl_annotation", retries=2, retry_delay_seconds=3)
def run_ensembl_annotation(chrom: str, pos: str, ref: str, alt: str) -> Optional[Dict[str, Any]]:
    """Fetch annotations from Ensembl VEP API."""
    run_logger = get_run_logger()
    try:
        # Remove 'chr' prefix if present for Ensembl API
        chrom_clean = chrom.replace('chr', '') if chrom.startswith('chr') else chrom
        
        # Format variant for VEP
        variant_string = f"{chrom_clean}:{pos}:{ref}/{alt}"
        
        url = "https://rest.ensembl.org/vep/human/hgvs"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        data = {"hgvs_notations": [variant_string]}
        
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if result:
            run_logger.info(f"Successfully fetched Ensembl annotations for {variant_string}")
            return {"source": "ensembl", "annotations": result[0] if result else {}}
        
        return None
    except Exception as e:
        run_logger.error(f"Error fetching Ensembl annotations: {e}")
        return None


@task(name="run_clinvar_annotation", retries=2, retry_delay_seconds=3)
def run_clinvar_annotation(chrom: str, pos: str, ref: str, alt: str) -> Optional[Dict[str, Any]]:
    """Fetch ClinVar annotations via NCBI E-utilities."""
    run_logger = get_run_logger()
    try:
        # Convert chromosome format
        chrom_clean = chrom.replace('chr', '') if chrom.startswith('chr') else chrom
        
        # Search ClinVar for the variant
        search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        search_params = {
            "db": "clinvar",
            "term": f"{chrom_clean}[Chromosome] AND {pos}[Base Position]",
            "retmode": "json",
            "retmax": "10"
        }
        
        search_response = requests.get(search_url, params=search_params, timeout=30)
        search_response.raise_for_status()
        search_data = search_response.json()
        
        if not search_data.get("esearchresult", {}).get("idlist"):
            run_logger.info("No ClinVar records found for variant")
            return None
        
        # Fetch details for the first result
        id_list = search_data["esearchresult"]["idlist"][:1]  # Take first result
        
        fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        fetch_params = {
            "db": "clinvar",
            "id": ",".join(id_list),
            "retmode": "json"
        }
        
        fetch_response = requests.get(fetch_url, params=fetch_params, timeout=30)
        fetch_response.raise_for_status()
        fetch_data = fetch_response.json()
        
        run_logger.info(f"Successfully fetched ClinVar annotations")
        return {"source": "clinvar", "annotations": fetch_data.get("result", {})}
        
    except Exception as e:
        run_logger.error(f"Error fetching ClinVar annotations: {e}")
        return None


@task(name="merge_annotations")
def merge_annotations(annotation_results: List[Optional[Dict[str, Any]]], config: AnnotationConfig) -> Dict[str, Any]:
    """Merge annotations from multiple sources based on priority."""
    run_logger = get_run_logger()
    
    merged_result = {
        "annotations": {},
        "variant": {},
        "sources_used": [],
        "metadata": {
            "total_sources": len([r for r in annotation_results if r is not None]),
            "config": {
                "enabled_sources": [s.source_name for s in config.enabled_sources],
                "parallel_execution": config.parallel_execution
            }
        }
    }
    
    # Sort results by source priority
    valid_results = [r for r in annotation_results if r is not None]
    
    for result in valid_results:
        source = result.get("source", "unknown")
        merged_result["sources_used"].append(source)
        
        if "annotations" in result:
            if source == "opencravat":
                # OpenCravat has nested structure
                merged_result["annotations"].update(result["annotations"])
                if "variant" in result:
                    merged_result["variant"].update(result["variant"])
            else:
                # Other sources have flatter structure
                merged_result["annotations"][source] = result["annotations"]
    
    run_logger.info(f"Merged annotations from {len(valid_results)} sources: {merged_result['sources_used']}")
    return merged_result


@task(name="convert_sqlite_to_json")
def convert_to_json(sqlite_file: str) -> Optional[Dict[str, Any]]:
    """
    Converts the OpenCravat SQLite output into a JSON-compatible dictionary.
    """
    import sqlite3
    run_logger = get_run_logger()
    
    try:
        conn = sqlite3.connect(sqlite_file)
        cursor = conn.cursor()

        result = {
            "source": "opencravat",
            "annotations": {},
            "variant": {}
        }

        cursor.execute("SELECT * FROM variant")
        variant_row = cursor.fetchone()
        if variant_row:
            columns = [col[0] for col in cursor.description]
            variant_data = dict(zip(columns, variant_row))
            for col in columns:
                if col.startswith("base__"):
                    json_key = col.replace("base__", "", 1)
                    result["variant"][json_key] = variant_data[col]

        cursor.execute("SELECT * FROM gene")
        gene_row = cursor.fetchone()
        if gene_row:
            gene_columns = [col[0] for col in cursor.description]
            gene_data = dict(zip(gene_columns, gene_row))

        annotator_names = set()

        cursor.execute("SELECT name FROM variant_annotator")
        for row in cursor.fetchall():
            annotator_names.add(row[0])

        cursor.execute("SELECT name FROM gene_annotator")
        for row in cursor.fetchall():
            annotator_names.add(row[0])

        # Skip these annotators
        skip_annotators = {"base", "original_input", "tagsampler"}

        for annotator in annotator_names:
            if annotator in skip_annotators:
                continue
            result["annotations"][annotator] = {}

            if gene_data and gene_columns:
                for col in gene_columns:
                    if col.startswith(f"{annotator}__"):
                        field = col.split("__", 1)[1]
                        if gene_data.get(col) is not None:
                            result["annotations"][annotator][field] = gene_data.get(col)

            if variant_row:
                for col in columns:
                    if col.startswith(f"{annotator}__"):
                        field = col.split("__", 1)[1]
                        if variant_data.get(col) is not None:
                            result["annotations"][annotator][field] = variant_data.get(col)
            

        run_logger.debug(f"Converted SQLite data to JSON for file {sqlite_file}")
        return result
    except Exception as e:
        run_logger.error(f"Error converting SQLite to JSON: {e}")
        return None
    finally:
        conn.close()


@task(name="cleanup_directory")
def clean_directory(output_dir: str) -> None:
    """Deletes all files in the output directory."""
    run_logger = get_run_logger()
    try:
        for file in os.listdir(output_dir):
            file_path = os.path.join(output_dir, file)
            os.remove(file_path)
        os.rmdir(output_dir) 
        run_logger.debug(f"Cleaned up and removed directory {output_dir}")
    except Exception as e:
        run_logger.warning(f"Error cleaning up directory {output_dir}: {e}")


@flow(name="variant_annotation_flow", task_runner=ConcurrentTaskRunner())
def variant_annotation_flow(
    variant_input: str, 
    annotators: List[str], 
    genome_build: str,
    annotation_config: AnnotationConfig
) -> Optional[Dict[str, Any]]:
    """
    Main Prefect flow for variant annotation with modular source selection.
    """
    run_logger = get_run_logger()
    run_logger.info(f"Starting variant annotation flow for {variant_input}")
    
    # Parse variant information
    chrom, pos, ref, alt = parse_variant(variant_input)
    
    # Prepare for annotations
    annotation_tasks = []
    
    # Sort sources by priority
    sorted_sources = sorted(annotation_config.enabled_sources, key=lambda x: x.priority)
    
    if annotation_config.parallel_execution:
        # Run all enabled annotation sources in parallel
        for source in sorted_sources:
            if source == AnnotationSource.OPENCRAVAT:
                output_dir = generate_temp_dir()
                tsv_file = create_tsv(chrom, pos, ref, alt, output_dir)
                task = run_opencravat.submit(tsv_file, annotators, output_dir, genome_build)
                annotation_tasks.append((task, output_dir))
            elif source == AnnotationSource.ENSEMBL:
                task = run_ensembl_annotation.submit(chrom, pos, ref, alt)
                annotation_tasks.append((task, None))
            elif source == AnnotationSource.CLINVAR:
                task = run_clinvar_annotation.submit(chrom, pos, ref, alt)
                annotation_tasks.append((task, None))
        
        # Collect results
        annotation_results = []
        cleanup_dirs = []
        
        for task, output_dir in annotation_tasks:
            try:
                result = task.result()
                annotation_results.append(result)
                if output_dir:
                    cleanup_dirs.append(output_dir)
            except Exception as e:
                run_logger.error(f"Annotation task failed: {e}")
                annotation_results.append(None)
                if output_dir:
                    cleanup_dirs.append(output_dir)
        
        # Clean up directories
        for cleanup_dir in cleanup_dirs:
            clean_directory.submit(cleanup_dir)
            
    else:
        # Run sources sequentially with fallback
        annotation_results = []
        for source in sorted_sources:
            try:
                if source == AnnotationSource.OPENCRAVAT:
                    output_dir = generate_temp_dir()
                    tsv_file = create_tsv(chrom, pos, ref, alt, output_dir)
                    result = run_opencravat(tsv_file, annotators, output_dir, genome_build)
                    clean_directory(output_dir)
                elif source == AnnotationSource.ENSEMBL:
                    result = run_ensembl_annotation(chrom, pos, ref, alt)
                elif source == AnnotationSource.CLINVAR:
                    result = run_clinvar_annotation(chrom, pos, ref, alt)
                else:
                    continue
                    
                annotation_results.append(result)
                
                # If we got a good result and fallback is disabled, break
                if result and not annotation_config.fallback_enabled:
                    break
                    
            except Exception as e:
                run_logger.error(f"Error with {source.source_name}: {e}")
                annotation_results.append(None)
    
    # Merge all annotation results
    final_result = merge_annotations(annotation_results, annotation_config)
    
    run_logger.info(f"Completed variant annotation flow for {variant_input}")
    return final_result


def process_summary(variant_input: str, annotators: List[str], db, user_id: str, hypothesis_id: str, genome_build: str, annotation_config: Optional[AnnotationConfig] = None):    
    """
    Enhanced process_summary function with Prefect flow integration and modular annotation sources.
    """
    if annotation_config is None:
        # Default configuration with all sources enabled
        annotation_config = AnnotationConfig(
            enabled_sources=[
                AnnotationSource.OPENCRAVAT,
                AnnotationSource.ENSEMBL,
                AnnotationSource.CLINVAR
            ],
            fallback_enabled=True,
            parallel_execution=True,
            timeout_seconds=300
        )
    
    if db.check_processing_status(variant_input):
        logger.info("Variant is already being annotated. Waiting for the result...")
        max_wait_time = 180  # 3 minutes timeout
        wait_time = 0
        while db.check_processing_status(variant_input):
            if wait_time >= max_wait_time:
                return {"message": "Processing timeout. Please try again later."}, 408
            time.sleep(1)
            wait_time += 1
        global_summary = db.check_global_summary(variant_input)

        if global_summary:
            logger.debug("Retrieved global summary from saved db")
            #if a user-specific summary does not already exist, create one using the global summary.
            user_summary = db.check_summary(user_id, hypothesis_id)

            if not user_summary:
                db.create_summary(user_id, hypothesis_id, global_summary)

            return {
                "summary": global_summary.get('summary'),
                "hypothesis_id": hypothesis_id,
                "user_id": user_id
            }, 200
        
        return db.get_summary(user_id, hypothesis_id) or {"message": "Report retrieval failed."}, 500
    
    # Mark variant as processing
    db.set_processing_status(variant_input, True)
    
    try:
        if not genome_build:
            logger.info("User didn't provide a genome build, defaulting to hg38")
            genome_build = "hg38"

        # Run the Prefect flow for annotation
        report = variant_annotation_flow(
            variant_input=variant_input,
            annotators=annotators,
            genome_build=genome_build,
            annotation_config=annotation_config
        )
    
    finally:
        # Mark as not processing
        db.set_processing_status(variant_input, False)
    
    if report is not None:
        db.create_global_summary(variant_input, report)
        return db.create_summary(user_id, hypothesis_id, report)
    
    return {"message": "Report generation failed."}, 500


# Utility function to create custom annotation configurations
def create_annotation_config(
    sources: List[str] = None,
    fallback_enabled: bool = True,
    parallel_execution: bool = True,
    timeout_seconds: int = 300
) -> AnnotationConfig:
    """
    Create a custom annotation configuration.
    
    Args:
        sources: List of source names ('opencravat', 'ensembl', 'clinvar', 'dbsnp')
        fallback_enabled: Whether to try fallback sources if primary fails
        parallel_execution: Whether to run sources in parallel
        timeout_seconds: Timeout for the entire annotation process
    """
    if sources is None:
        sources = ['opencravat', 'ensembl', 'clinvar']
    
    enabled_sources = []
    source_mapping = {
        'opencravat': AnnotationSource.OPENCRAVAT,
        'ensembl': AnnotationSource.ENSEMBL,
        'clinvar': AnnotationSource.CLINVAR,
        'dbsnp': AnnotationSource.DBSNP
    }
    
    for source_name in sources:
        if source_name in source_mapping:
            enabled_sources.append(source_mapping[source_name])
    
    return AnnotationConfig(
        enabled_sources=enabled_sources,
        fallback_enabled=fallback_enabled,
        parallel_execution=parallel_execution,
        timeout_seconds=timeout_seconds
    )






    

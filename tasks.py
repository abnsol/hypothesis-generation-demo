from prefect import task
from datetime import datetime, timezone
from uuid import uuid4
from socketio_instance import socketio
from enum import Enum
from status_tracker import status_tracker, TaskState
from utils import emit_task_update
from loguru import logger

### Enrich Tasks
@task(retries=2, cache_policy=None)
def check_enrich(db, current_user_id, phenotype, variant, hypothesis_id):
    try:

        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.STARTED,
            progress=0  
        )
        
        if db.check_enrich(current_user_id, phenotype, variant):
            enrich = db.get_enrich_by_phenotype_and_variant(phenotype, variant, current_user_id)
            
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verifying existence of enrichment data",
                state=TaskState.COMPLETED,
                progress=80,
                details={"found": True, "enrich_id": enrich["id"]}
            )
            return enrich
            
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.COMPLETED,
            details={"found": False},
            next_task="Getting candidate genes"
        )
        return None
        
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of enrichment data",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def get_candidate_genes(prolog_query, variant, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.STARTED,
            next_task="Predicting causal gene",
        )

        print("Executing: get candidate genes")
        result = prolog_query.get_candidate_genes(variant)
        
        # Check if any candidate genes were found
        if not result:
            error_msg = f"No candidate genes found for variant {variant}. This variant may not be in the knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Getting candidate genes",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.COMPLETED,
            details={"genes_count": len(result)}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting candidate genes",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def predict_causal_gene(llm, phenotype, candidate_genes, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.STARTED,
            next_task="Getting relevant gene proof"
        )

        print("Executing: predict causal gene")
        result = llm.predict_casual_gene(phenotype, candidate_genes)["causal_gene"]

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.COMPLETED,
            details={"predicted_gene": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Predicting causal gene",
            state=TaskState.FAILED,
            error=str(e)
        )
        raise

@task(retries=2)
def get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.STARTED,
            next_task="Creating enrich data"
        )

        print("Executing: get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        
        # Log the result for debugging
        if result[0] is None:
            logger.warning(f"Initial gene proof query failed for variant {variant} and gene {causal_gene}. Reason: {result[1]}. Will attempt retry.")
        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.COMPLETED,
            details={"relevant_gene_proof": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting relevant gene proof",
            state=TaskState.FAILED,
            next_task="Retrying to predict causal gene",
            error=str(e)          
        )
        raise

@task(retries=2)
def retry_predict_causal_gene(llm, phenotype, candidate_genes, proof, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.RETRYING,
            next_task="Retrying to get relevant gene proof"
        )

        print(f"Retrying predict causal gene with proof: {proof}")
        result = llm.predict_casual_gene(phenotype, candidate_genes, rule=proof, prev_gene=causal_gene)["causal_gene"]

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.COMPLETED,
            details={"retry_predict_causal_gene": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to predict causal gene",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def retry_get_relevant_gene_proof(prolog_query, variant, causal_gene, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.RETRYING,
            next_task="Creating enrich data"
        )

        print("Retrying get relevant gene proof")
        result = prolog_query.get_relevant_gene_proof(variant, causal_gene)
        
        # Check if the retry still failed
        if result[0] is None:  # result is (causal_graph, proof)
            error_msg = f"Retry failed: Unable to generate causal graph for variant {variant} and gene {causal_gene}. Reason: {result[1]}"
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Retrying to get relevant gene proof",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)
       
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.COMPLETED,
            details={"retry_relevant_gene_proof": result}
        ) 
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Retrying to get relevant gene proof",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None)
def create_enrich_data(db, variant, phenotype, causal_gene, relevant_gos, causal_graph, current_user_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.STARTED
        )

        print("Creating enrich data in the database")
        enrich_data = {
            "id": str(uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            "variant": variant,
            "phenotype": phenotype,
            "causal_gene": causal_gene,
            "GO_terms": relevant_gos,
            "causal_graph": causal_graph
        }
        db.create_enrich(current_user_id, enrich_data)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.COMPLETED,
            details={"enrichment_id": enrich_data["id"]}
        )
        
        # hypothesis_history = status_tracker.get_history(hypothesis_id)
        # print("Updating hypothesis in the database...")
        # hypothesis_data = {
        #         "task_history": hypothesis_history,
        #     }
        # db.update_hypothesis(hypothesis_id, hypothesis_data)

        return enrich_data["id"]
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Creating enrich data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

### Hypothesis Tasks
@task(cache_policy=None, retries=2)
def check_hypothesis(db, current_user_id, enrich_id, go_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.STARTED,
            next_task="Getting enrichement data"
        )

        print("Checking hypothesis data")
        if db.check_hypothesis(current_user_id, enrich_id, go_id):
            hypothesis = db.get_hypothesis_by_enrich_and_go(enrich_id, go_id, current_user_id)
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Verifying existence of hypothesis data",
                state=TaskState.COMPLETED,
                progress=100,
                details={"found": True}
            )
            return hypothesis
        
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existence of hypothesis data",
            state=TaskState.COMPLETED,
            details={"found": False}
        )
        return None
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Verifying existance of hypothesis data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None, retries=2)
def get_enrich(db, current_user_id, enrich_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.STARTED,
            next_task="Getting gene data"
        )

        print("Fetching enrich data...")
        result = db.get_enrich(current_user_id, enrich_id)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.COMPLETED,
            details={"get_enrich": result}
        )
        return result

    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting enrichement data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def get_gene_ids(prolog_query, gene_names, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.STARTED,
            next_task="Querying gene data"
        )
        print("Fetching gene IDs...")
        
        result = prolog_query.get_gene_ids(gene_names)
        
        # Check if any gene IDs were found
        if not result:
            error_msg = f"No gene IDs found for genes: {gene_names}. These genes may not be in the knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Getting gene data",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)
        
        # Check for None values in the result
        if any(gene_id is None for gene_id in result):
            error_msg = f"Some gene IDs are missing for genes: {gene_names}. Incomplete data in knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Getting gene data",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.COMPLETED,
            details={"get_gene_ids": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Getting gene data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_gene_query(prolog_query, query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.STARTED,
            next_task="Querying variant data"
        )

        print("Executing Prolog query to retrieve gene names...")
        result = prolog_query.execute_query(query)
        
        # Check if the query returned None (failed)
        if result is None:
            error_msg = f"Gene query failed: {query}. Gene data may not be available in knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Querying gene data",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.COMPLETED,
            details={"execute_gene_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying gene data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_variant_query(prolog_query, query, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.STARTED,
            next_task="Querying phenotype data"
        )
        print("Executing Prolog query to retrieve variant ids...")
        result = prolog_query.execute_query(query)
        
        # Check if the query returned None (failed)
        if result is None:
            error_msg = f"Variant query failed: {query}. Variant data may not be available in knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Querying variant data",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.COMPLETED,
            details={"execute_variant_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying variant data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def execute_phenotype_query(prolog_query, phenotype, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.STARTED,
            next_task="Generating graph summary"
        )
        print("Executing Prolog query to retrieve phenotype id...")
        result = prolog_query.execute_query(f"term_name(efo(X), {phenotype})")
        
        # Check if the query returned None (failed)
        if result is None:
            error_msg = f"Phenotype query failed for: {phenotype}. Phenotype may not be available in knowledge base."
            emit_task_update(
                hypothesis_id=hypothesis_id,
                task_name="Querying phenotype data",
                state=TaskState.FAILED,
                error=error_msg
            )
            raise ValueError(error_msg)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.COMPLETED,
            details={"execute_phenotype_query": result}
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Querying phenotype data",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(retries=2)
def summarize_graph(llm, causal_graph, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.STARTED,
            next_task="Generating hypothesis"
        )

        print("Summarizing causal graph...")
        result = llm.summarize_graph(causal_graph)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.COMPLETED
        )
        return result
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating graph summary",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

@task(cache_policy=None, retries=2)
def create_hypothesis(db, enrich_id, go_id, variant_id, phenotype, causal_gene, causal_graph, summary, current_user_id, hypothesis_id):
    try:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.STARTED,
            details={"go_id": go_id}
        )
        hypothesis_history = status_tracker.get_history(hypothesis_id)
        print("Creating hypothesis in the database...")
        hypothesis_data = {
                "enrich_id": enrich_id,
                "go_id": go_id,
                "phenotype": phenotype,
                "causal_gene": causal_gene,
                "graph": causal_graph,
                "summary": summary,
                "biological_context": "",
                "status": "completed"
            }
        db.update_hypothesis(hypothesis_id, hypothesis_data)

        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.COMPLETED,
            details={
                "status": "completed",
                "hypothesis_id": hypothesis_id, 
                "go_id": go_id
            }
        )
        # hypothesis_history = status_tracker.get_history(hypothesis_id)
        # print("Updating hypothesis in the database...")
        # hypothesis_data = {
        #         "task_history": hypothesis_history,
        #     }
        # db.update_hypothesis(hypothesis_id, hypothesis_data)
        
        return hypothesis_id
    except Exception as e:
        emit_task_update(
            hypothesis_id=hypothesis_id,
            task_name="Generating hypothesis",
            state=TaskState.FAILED,
            error=str(e)          
        )
        raise

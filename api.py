from threading import Timer
import uuid
from prefect.utilities.asyncutils import sync_compatible
from prefect.context import get_run_context
from flask import Flask, json, request, jsonify
from flask_jwt_extended import get_jwt_identity
from flask_restful import Resource, Api, reqparse
from flask_socketio import join_room
from socketio_instance import socketio
from auth import socket_token_required, token_required
from datetime import datetime, timezone
from uuid import uuid4
from flows import async_enrichment_process, enrichment_flow, hypothesis_flow
from status_tracker import status_tracker, TaskState
from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner
from utils import emit_task_update
from loguru import logger
from prefect.client import get_client
import os
import asyncio
import logging

logger = logging.getLogger(__name__)

async def run_enrichment_deployment(parameters, work_queue_name):
    """
    Asynchronously run a Prefect deployment
    
    :param parameters: Parameters to pass to the flow
    :param work_queue_name: Name of the work queue
    :return: Flow run object
    """
    try:
        client = get_client()
        
        # Create a flow run directly with only serializable parameters
        flow_run = await client.create_flow_run(
            flow=async_enrichment_process,
            parameters={
                "current_user_id": parameters["current_user_id"],
                "phenotype": parameters["phenotype"],
                "variant": parameters["variant"],
                "hypothesis_id": parameters["hypothesis_id"]
            },
            tags=["enrichment-process"]
        )
        
        logger.info(f"Created flow run: {flow_run.id}")
        return flow_run
    
    except Exception as e:
        logger.error(f"Error running deployment: {e}")
        raise

class EnrichAPI(Resource):
    def __init__(self, enrichr, llm, prolog_query, db, work_queue_name):
        self.enrichr = enrichr
        self.llm = llm
        self.prolog_query = prolog_query
        self.db = db
        self.work_queue_name = work_queue_name

    @token_required
    def get(self, current_user_id):
        # Get the enrich_id from the query parameters
        enrich_id = request.args.get('id')
        if enrich_id:
            # Fetch a specific enrich by enrich_id and user_id
            enrich = self.db.get_enrich(current_user_id, enrich_id)
            if not enrich:
                return {"message": "Enrich not found or access denied."}, 404
            return enrich, 200
          
        # Fetch all hypotheses for the current user
        enrich = self.db.get_enrich(user_id=current_user_id)
        return enrich, 200

    @token_required
    def post(self, current_user_id):
        try:
            args = request.args
            phenotype, variant = args['phenotype'], args['variant']
            existing_hypothesis = self.db.get_hypothesis_by_phenotype_and_variant(current_user_id, phenotype, variant)
            
            # Prepare only serializable parameters
            deployment_params = {
                "current_user_id": current_user_id,
                "phenotype": phenotype,
                "variant": variant
            }
            
            if existing_hypothesis:
                # Use existing hypothesis
                deployment_params["hypothesis_id"] = existing_hypothesis['id']
                hypothesis_id = existing_hypothesis['id']
            else:
                # Generate hypothesis_id immediately
                hypothesis_id = str(uuid.uuid4())
                # Store initial hypothesis state
                hypothesis_data = {
                    "id": hypothesis_id,
                    "phenotype": phenotype,
                    "variant": variant,
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
                    "task_history": [],
                }
                # Save initial hypothesis state
                self.db.create_hypothesis(hypothesis_data, current_user_id)
                deployment_params["hypothesis_id"] = hypothesis_id
            
            # Create and run the async task
            async def run_flow():
                try:
                    print("Running flow...")
                    flow_run = await run_enrichment_deployment(
                        parameters=deployment_params,
                        work_queue_name=self.work_queue_name
                    )
                    logger.info(f"Flow run created with ID: {flow_run.id}")
                except Exception as e:
                    logger.error(f"Failed to create flow run: {e}")
                    # Update hypothesis status to failed
                    self.db.update_hypothesis(hypothesis_id, {
                        "status": "failed",
                        "error": str(e),
                        "updated_at": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
                    })
                    raise
        
            # Run the async function
            asyncio.run(run_flow())
            return {"hypothesis_id": hypothesis_id}, 202
            
        except Exception as e:
            logger.error(f"Error in post request: {e}")
            return {"error": str(e)}, 500

    @token_required
    def delete(self, current_user_id):
        enrich_id = request.args.get('id')
        if enrich_id:
            result = self.db.delete_enrich(current_user_id, enrich_id)
            return result, 200
        return {"message": "enrich id is required!"}, 400


class HypothesisAPI(Resource):
    def __init__(self, enrichr, prolog_query, llm, db):
        self.enrichr = enrichr
        self.prolog_query = prolog_query
        self.llm = llm
        self.db = db

    @token_required
    def get(self, current_user_id):
        # Get the hypothesis_id from the query parameters
        hypothesis_id = request.args.get('id')

        if hypothesis_id:
            # Fetch a specific hypothesis by hypothesis_id and user_id
            hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                return {"message": "Hypothesis not found or access denied."}, 404
            
            # Check if enrichment is complete
            required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
            is_complete = all(field in hypothesis for field in required_fields)
            # Get task history
            task_history = status_tracker.get_history(hypothesis_id)
            for task in task_history:
                task.pop('details', None)  # Remove 'details' if it exists

            # Get only pending tasks from task history
            pending_tasks = [task for task in task_history if task.get('state') == TaskState.STARTED.value]
            last_pending_task = [pending_tasks[-1]] if pending_tasks else [] 
            print(f"last_pending_task: {last_pending_task}")

            if is_complete:
                enrich_id = hypothesis.get('enrich_id')
                enrich_data = self.db.get_enrich(current_user_id, enrich_id)
                # Remove 'causal_graph' field from enrich_data if it exists
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                return {
                    'id': hypothesis_id,
                    'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                    'enrich_id': enrich_id,
                    'phenotype': hypothesis['phenotype'],
                    "status": "completed",
                    "created_at": hypothesis.get('created_at'),
                    "result": enrich_data
                }, 200

            latest_state = status_tracker.get_latest_state(hypothesis_id)
            
            status_data = {
                'id': hypothesis_id,
                'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                'phenotype': hypothesis['phenotype'],
                'status': 'pending',
                "created_at": hypothesis.get('created_at'),
                'task_history': last_pending_task,
            }
            if 'enrich_id' in hypothesis and hypothesis.get('enrich_id') is not None:
                enrich_id = hypothesis.get('enrich_id')
                status_data['enrich_id'] = enrich_id
                enrich_data = self.db.get_enrich(current_user_id, enrich_id)
                if isinstance(enrich_data, dict):
                    enrich_data.pop('causal_graph', None)
                status_data['result'] = enrich_data
                

            # Check for failed state
            if latest_state and latest_state.get('state') == 'failed':
                status_data['status'] = 'failed'
                status_data['error'] = latest_state.get('error')

            return status_data, 200

        # Fetch all hypotheses for the current user
        hypotheses = self.db.get_hypotheses(user_id=current_user_id)
        
        # Filter and format the response for all hypotheses
        formatted_hypotheses = []
        for hypothesis in hypotheses:
            # Get only pending tasks from task history
            pending_tasks = [
                task for task in status_tracker.get_history(hypothesis['id']) 
                if task.get('state') == TaskState.STARTED.value
            ]
            last_pending_task = [pending_tasks[-1]] if pending_tasks else []
            
            formatted_hypothesis = {
                'id': hypothesis['id'],
                'phenotype': hypothesis['phenotype'],
                'variant': hypothesis.get('variant') or hypothesis.get('variant_id'),
                'created_at': hypothesis.get('created_at'),
                'status': hypothesis.get('status'),
                'task_history': last_pending_task
            }
            if 'enrich_id' in hypothesis and hypothesis.get('enrich_id') is not None:
                 formatted_hypothesis['enrich_id'] = hypothesis.get('enrich_id')
            if 'biological_context' in hypothesis and hypothesis.get('biological_context') is not None:
                formatted_hypothesis['biological_context'] = hypothesis.get('biological_context')
            if 'causal_gene' in hypothesis and hypothesis.get('causal_gene') is not None:
                formatted_hypothesis['causal_gene'] = hypothesis.get('causal_gene')
            formatted_hypotheses.append(formatted_hypothesis)
            
        return formatted_hypotheses, 200
        # return hypotheses, 200

    @token_required
    def post(self, current_user_id):
        enrich_id = request.args.get('id')
        go_id = request.args.get('go')

        # Get the hypothesis associated with this enrichment
        hypothesis = self.db.get_hypothesis_by_enrich(current_user_id, enrich_id)
        if not hypothesis:
            return {"message": "No hypothesis found for this enrichment"}, 404
        
        hypothesis_id = hypothesis['id']
        
        # Run the Prefect flow and return the result
        flow_result = hypothesis_flow(current_user_id, hypothesis_id, enrich_id, go_id, self.db, self.prolog_query, self.llm)

        return flow_result[0], flow_result[1]

    
    @token_required
    def delete(self, current_user_id):
        hypothesis_id = request.args.get('hypothesis_id')
        if hypothesis_id:
            return self.db.delete_hypothesis(current_user_id, hypothesis_id)
        return {"message": "Hypothesis ID is required"}, 400
        
    
class BulkHypothesisDeleteAPI(Resource):
    def __init__(self, db):
        self.db = db
        
    @token_required
    def post(self, current_user_id):
        data = request.get_json()
        
        if not data or 'hypothesis_ids' not in data:
            return {"message": "hypothesis_ids is required in request body"}, 400
            
        hypothesis_ids = data.get('hypothesis_ids')
        
        # Validate the list of IDs
        if not isinstance(hypothesis_ids, list):
            return {"message": "hypothesis_ids must be a list"}, 400
            
        if not hypothesis_ids:
            return {"message": "hypothesis_ids list cannot be empty"}, 400
            
        # Call the bulk delete method
        result, status_code = self.db.bulk_delete_hypotheses(current_user_id, hypothesis_ids)

        return result, status_code

class ChatAPI(Resource):
    def __init__(self, llm, db):
        self.llm = llm
        self.db = db

    @token_required
    def post(self, current_user_id):
        query = request.form.get('query')
        hypothesis_id = request.form.get('hypothesis_id')

        hypothesis = self.db.get_hypotheses(current_user_id, hypothesis_id)
        print(f"Hypothesis: {hypothesis}")
        
        if not hypothesis:
            return {"error": "Hypothesis not found or access denied"}, 404
        
        graph = hypothesis.get('graph')
        response = self.llm.chat(query, graph)
        response = {"response": response}
        return response

def init_socket_handlers(db_instance):
    logger.info("Initializing socket handlers...")
    # @socketio.on('connect')
    # def handle_connect():
    #     logger.info("Client connected")
    #     print("Client connected")
    #     return True
    
    @socketio.on('connect')
    @socket_token_required
    def handle_connect(self, current_user_id):
        logger.info("somehting else")
        logger.info(f"Client connected: {current_user_id}")
        client_id = request.sid
        inactivity_timer = Timer(300, lambda: socketio.close_room(client_id))
        inactivity_timer.start()

        return True

    @socketio.on('disconnect')
    def handle_disconnect():
        logger.info("Client disconnected")

    @socketio.on('subscribe_hypothesis')
    @socket_token_required
    def handle_subscribe(data, current_user_id):
        try:
            logger.info(f"Received subscribe request: {data}")
            print(f"Received subscribe request: {data}")
            # Handle both string and dict input
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON string: {data}")
                    return {'error': 'Invalid JSON format'}, 400
            
            # Validate input
            if not isinstance(data, dict) or 'hypothesis_id' not in data:
                logger.error(f"Invalid data format: {data}")
                return {'error': 'Expected format: {"hypothesis_id": "value"}'}, 400
                
            hypothesis_id = data.get('hypothesis_id')
            if not hypothesis_id:
                logger.error("Missing hypothesis_id")
                return {'error': 'hypothesis_id is required'}, 400
            
            response_data = {
                'hypothesis_id': hypothesis_id,
                'timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
            }
                
                
            # Join a room specific to this hypothesis
            room = f"hypothesis_{hypothesis_id}"    
            join_room(room)
            logger.info(f"Joined room: {room}")

            # Verify room membership
            rooms = socketio.server.rooms(request.sid)
            logger.info(f"Current rooms for client {request.sid}: {rooms}")
            
            # Get hypothesis data
            hypothesis = db_instance.get_hypotheses(current_user_id, hypothesis_id)
            if not hypothesis:
                logger.error(f"Hypothesis not found: {hypothesis_id}")
                raise ValueError("Hypothesis not found or access denied")

            # Get task history
            task_history = status_tracker.get_history(hypothesis_id)
            response_data['task_history'] = task_history
                
            # Check if hypothesis is complete
            required_fields = ['enrich_id', 'go_id', 'summary', 'graph']
            is_complete = all(field in hypothesis for field in required_fields)  
            
            if is_complete:
                response_data.update({
                    'status': 'completed',
                    'result': hypothesis,
                    'progress': 100
                })
            else:
                latest_state = status_tracker.get_latest_state(hypothesis_id)
                progress = status_tracker.calculate_progress(task_history)
                current_task = latest_state['task'] if latest_state else None
                error = latest_state.get('error') if latest_state and latest_state.get('state') == TaskState.FAILED else None
                
                response_data.update({
                    'status': 'pending',
                    'progress': progress
                })
            if current_task:
                response_data['current_task'] = current_task
            if error:
                response_data['error'] = error
            
            
            logger.info(f"Emitting task_update: {response_data}")
            socketio.emit('task_update', response_data, room=room)
            return {"status": "subscribed", "room": room}
            
        except Exception as e:
            logger.error(f"Error in handle_subscribe: {str(e)}")
            return {"error": str(e)}, 500

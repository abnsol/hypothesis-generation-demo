from datetime import datetime, timezone
import eventlet
from socketio_instance import socketio
from status_tracker import status_tracker, TaskState
from prefect.context import get_run_context
from loguru import logger

def emit_task_update(hypothesis_id, task_name, state, progress=0, details=None, next_task=None, error=None):
    """
    Emits real-time updates via WebSocket with retry mechanism
    """
    task_history = status_tracker.get_history(hypothesis_id)

    # Filter to only include 'started' state entries and keep the latest 5
    filtered_history = [entry for entry in task_history if entry["state"] == "completed"]
    latest_5_started_tasks = filtered_history[-5:]

    if progress == 0:
        progress = status_tracker.calculate_progress(task_history)

    # Update the status tracker first
    status_tracker.add_update(hypothesis_id, progress, task_name, state, details, error)

    room = f"hypothesis_{hypothesis_id}"
    update = {
        "hypothesis_id": hypothesis_id,
        "timestamp": datetime.now(timezone.utc).isoformat(timespec='milliseconds') + "Z",
        "task": task_name,
        "state": state.value,
        "progress": progress,
        "task_history": latest_5_started_tasks
    }

    if next_task:
        update["next_task"] = next_task
    if error:
        update["error"] = error
        update["status"] = "failed"
    
    # Handle completion status
    if state == TaskState.COMPLETED:
        if task_name == "Creating enrich data" or (task_name =="Verifying existence of enrichment data" and progress == 80):
            update["status"] = "Enrichment_completed"
            update["progress"] = 80  # enrichment completion is 80%
        elif task_name == "Generating hypothesis" or (task_name == "Verifying existence of hypothesis data" and progress == 100):
            update["status"] = "Hypothesis_completed"
            update["progress"] = 100  # hypothesis completion is 100%
    elif state == TaskState.FAILED:
        update["status"] = "failed"
        update["error"] = error

    # Emit with retry mechanism
    max_retries = 3
    for attempt in range(max_retries):
        try:
            socketio.emit('task_update', update, room=room)
            logger.info(f"Emitted task update to room {room}")
            break
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to emit task update after {max_retries} attempts: {e}")
                raise
            logger.warning(f"Emit attempt {attempt + 1} failed: {e}")
            socketio.sleep(1)  # Wait before retry
    
    socketio.sleep(0)
import azure.functions as func
import json
import yaml
import os
import uuid
from datetime import datetime

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

CONFIG_DIR = "config"
CONFIG_FILE = os.path.join(CONFIG_DIR, "pipeline_config.yaml")

os.makedirs(CONFIG_DIR, exist_ok=True)


def make_response(data, status_code=200):
    return func.HttpResponse(
        json.dumps(data, indent=2),
        status_code=status_code,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        }
    )


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {
            "pipelines": []
        }

    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)

    return data if data else {"pipelines": []}


def save_config(data):
    with open(CONFIG_FILE, "w", encoding="utf-8") as file:
        yaml.dump(
            data,
            file,
            sort_keys=False,
            default_flow_style=False
        )


@app.route(route="metadata", methods=["GET", "POST", "OPTIONS"])
def metadata(req: func.HttpRequest) -> func.HttpResponse:

    if req.method == "OPTIONS":
        return make_response({}, 204)

    action = req.params.get("action")

    try:

        # ----------------------------
        # CREATE PIPELINE
        # ----------------------------
        if action == "create_pipeline" and req.method == "POST":

            body = req.get_json()
            run_id = str(uuid.uuid4())[:8]

            pipeline = {
                "run_id": run_id,
                "pipeline_status": "Created",
                "pipeline_name": body.get("pipeline_name"),
                "environment": body.get("environment"),
                "dependency": body.get("dependency"),
                "execution_mode": body.get("execution_mode", "batch"),
                "created_at": datetime.now().isoformat(),

                "source": body.get("source", {}),
                "target": body.get("target", {}),
                "adf": body.get("adf", {}),
                "config": body.get("config", {}),
                "storage_zones": body.get("storage_zones", {}),

                "resource_consumed_cost": {
                    "azure_functions": 0.00,
                    "azure_data_factory": 0.00,
                    "azure_databricks": 0.00,
                    "azure_sql_db": 0.00,
                    "linked_services": 0.00,
                    "azure_key_vault": 0.00,
                    "adls_storage": 0.00,
                    "total": 0.00
                }
            }

            config_data = load_config()

            if "pipelines" not in config_data:
                config_data["pipelines"] = []

            config_data["pipelines"].append(pipeline)

            save_config(config_data)

            return make_response({
                "status": "success",
                "message": "Pipeline created and config YAML generated",
                "run_id": run_id,
                "pipeline_status": "Created",
                "config_file": CONFIG_FILE,
                "pipeline": pipeline
            }, 201)

        # ----------------------------
        # GET FULL CONFIG YAML DATA
        # ----------------------------
        if action == "config" and req.method == "GET":
            return make_response(load_config(), 200)

        # ----------------------------
        # GET PIPELINE CONSUMED COST
        # This is called by index.html:
        # action=pipeline_cost&run_id=<run_id>
        # ----------------------------
        if action == "pipeline_cost" and req.method == "GET":

            run_id = req.params.get("run_id")

            if not run_id:
                return make_response({
                    "status": "failed",
                    "message": "run_id is required"
                }, 400)

            config_data = load_config()

            for pipeline in config_data.get("pipelines", []):

                if pipeline.get("run_id") == run_id:

                    # Demo consumed cost after completion
                    # Later replace this with actual Azure Cost Management API result
                    consumed_cost = {
                        "azure_functions": 0.0002,
                        "azure_data_factory": 0.25,
                        "azure_databricks": 2.40,
                        "azure_sql_db": 1.60,
                        "linked_services": 0.00,
                        "azure_key_vault": 0.003,
                        "adls_storage": 0.10,
                        "total": 4.3532
                    }

                    pipeline["pipeline_status"] = "Completed"
                    pipeline["completed_at"] = datetime.now().isoformat()
                    pipeline["resource_consumed_cost"] = consumed_cost

                    save_config(config_data)

                    return make_response({
                        "status": "success",
                        "run_id": run_id,
                        "pipeline_status": "Completed",
                        "resource_consumed_cost": consumed_cost
                    }, 200)

            return make_response({
                "status": "failed",
                "message": "run_id not found"
            }, 404)

        return make_response({
            "status": "failed",
            "message": "Invalid request"
        }, 400)

    except Exception as e:
        return make_response({
            "status": "error",
            "message": str(e)
        }, 500)
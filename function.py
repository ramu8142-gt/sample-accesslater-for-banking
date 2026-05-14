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
        return {"pipelines": []}

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

    try:
        action = req.params.get("action")

        if action == "create_pipeline" and req.method == "POST":
            body = req.get_json()
            run_id = str(uuid.uuid4())[:8]

            pipeline = {
                "run_id": run_id,
                "pipeline_name": body.get("pipeline_name"),
                "environment": body.get("environment"),
                "dependency": body.get("dependency"),
                "created_at": datetime.now().isoformat(),
                "execution_mode": body.get("execution_mode", "batch"),

                "source": body.get("source", {}),
                "target": body.get("target", {}),

                "adf": body.get("adf", {
                    "factory_name": os.getenv(
                        "ADF_FACTORY_NAME",
                        "adf-acl-ztetl-dev-in-01"
                    ),
                    "pipeline_name": os.getenv(
                        "ADF_PIPELINE_NAME",
                        "zero_touch_etl_pipeline"
                    )
                }),

                "config": body.get("config", {
                    "file_name": "pipeline_config.yaml",
                    "storage_container": "config"
                }),

                "storage_zones": body.get("storage_zones", {
                    "raw": "raw/",
                    "bronze": "bronze/",
                    "silver": "silver/",
                    "gold": "gold/",
                    "quarantine": "quarantine/",
                    "audit": "audit/",
                    "config": "config/"
                }),

                "azure_resources": body.get("azure_resources", {})
            }

            config_data = load_config()

            if "pipelines" not in config_data:
                config_data["pipelines"] = []

            config_data["pipelines"].append(pipeline)

            save_config(config_data)

            return make_response({
                "status": "success",
                "run_id": run_id,
                "message": "Pipeline config generated successfully",
                "config_file": CONFIG_FILE,
                "pipeline": pipeline
            }, 201)

        if action == "config" and req.method == "GET":
            return make_response(load_config(), 200)

        return make_response({
            "status": "failed",
            "message": "Invalid request"
        }, 400)

    except Exception as e:
        return make_response({
            "status": "error",
            "message": str(e)
        }, 500)
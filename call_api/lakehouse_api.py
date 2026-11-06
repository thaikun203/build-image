"""
lakehouse_api.py
Tổng hợp tất cả hàm gọi API từ RegistryFeature, RegistryModel, FeatureData.
Import file này để dùng chung trong notebook/pipeline.

Usage:
    from lakehouse_api import get_bearer_token, registry_feature, register_model, ...
    from lakehouse_api import _rm_setup_mlflow, registry_model
"""

import os
import json
import requests
import pandas as pd
from typing import Optional, Tuple

try:
    import mlflow
except ImportError:
    mlflow = None

# ─────────────────────────────────────────────
# CONFIG (đọc từ env hoặc override trực tiếp)
# ─────────────────────────────────────────────

LOGIN_URL   = os.environ.get("LOGIN_URL", "")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "")
MODEL_API   = os.environ.get("MODEL_API", "http://10.168.6.107:8110/api/v1/models")


# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────

def get_bearer_token() -> str:
    """
    Lấy access_token từ Keycloak.
    Đọc thông tin đăng nhập từ env:
        LOGIN_URL, username_login, password_login, CLIENT_SECRET
    """
    try:
        response = requests.post(
            f"{LOGIN_URL}/realms/data-platform/protocol/openid-connect/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "username":      os.environ.get("username_login"),
                "password":      os.environ.get("password_login"),
                "client_id":     "data-platform",
                "client_secret": os.environ.get("CLIENT_SECRET"),
                "grant_type":    "password",
            },
            timeout=30,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise Exception("Cannot get access_token")
        return token
    except requests.exceptions.RequestException as e:
        print("Error during API login:", str(e))
        raise


# ─────────────────────────────────────────────
# USER INFO
# ─────────────────────────────────────────────

def get_user_info(token: str) -> dict:
    """
    POST /admin/v1/user/get-user-info
    Trả về dict data (bao gồm organizationId).
    """
    url = f"{GATEWAY_URL}/admin/v1/user/get-user-info"
    response = requests.post(url, headers={"Authorization": f"Bearer {token}"})
    if response.status_code == 200:
        return response.json().get("data", {})
    raise Exception(f"Cannot get user info: {response.status_code} – {response.text}")


def get_organization_id(token: str) -> str:
    """Shortcut lấy organizationId từ user info."""
    return get_user_info(token).get("organizationId")


# ─────────────────────────────────────────────
# CATALOG / SCHEMA / TABLE  (spark-session API)
# ─────────────────────────────────────────────

def _list_spark_session(token: str, catalog_name: str = "", schema_name: str = "") -> list:
    """
    GET /lake/workspace-service/v1/catalog/spark-session
    Trả về list tên (catalog / schema / table tuỳ params).
    """
    organization_id = get_organization_id(token)
    params = {
        "workspaceName": os.environ.get("WORKSPACE_NAME"),
        "catalogName":   catalog_name,
        "schemaName":    schema_name,
        "organizationId": organization_id,
    }
    response = requests.get(
        f"{GATEWAY_URL}/lake/workspace-service/v1/catalog/spark-session",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
    )
    response.raise_for_status()
    return [item["name"] for item in response.json().get("object", [])]


def list_catalogs(token: str) -> list:
    """Trả về danh sách catalog."""
    return _list_spark_session(token, catalog_name="", schema_name="")


def list_schemas(token: str, catalog_name: str) -> list:
    """Trả về danh sách schema trong catalog."""
    return _list_spark_session(token, catalog_name=catalog_name, schema_name="")


def list_tables(token: str, catalog_name: str, schema_name: str) -> list:
    """Trả về danh sách table trong schema."""
    return _list_spark_session(token, catalog_name=catalog_name, schema_name=schema_name)


# ─────────────────────────────────────────────
# FEATURE REGISTRY
# ─────────────────────────────────────────────

def get_input_schema(df: pd.DataFrame, selected_cols: list) -> dict:
    """Tạo dict {column_name: dtype} từ DataFrame."""
    return {col: str(df[col].dtype) for col in selected_cols}


def registry_feature(
    feature_group_id: str,
    pipeline_ver_id: str,
    workspace_id: str,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    token: str,
    schema_define: dict,
) -> dict:
    """
    POST /inference/v1/feature-versions
    Đăng ký feature version lên lakehouse API.
    """
    url = f"{GATEWAY_URL}/inference/v1/feature-versions"
    body = {
        "featureGroupId":    feature_group_id,
        "pipelineVersionId": pipeline_ver_id,
        "workspaceId":       workspace_id,
        "catalogName":       catalog_name,
        "schemaName":        schema_name,
        "tableName":         table_name,
        "schemaDefinition":  schema_define,
        "metadata":          "owner=data-team",
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}",
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body), timeout=30)
        response.raise_for_status()
        return {"success": True, "status_code": response.status_code, "data": response.json()}
    except requests.exceptions.HTTPError as e:
        return {"success": False, "error": f"HTTP error: {str(e)}", "response_text": response.text}
    except requests.exceptions.ConnectionError as e:
        return {"success": False, "error": f"Connection error: {str(e)}"}
    except requests.exceptions.Timeout as e:
        return {"success": False, "error": f"Timeout error: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


# ─────────────────────────────────────────────
# MODEL REGISTRY  (MODEL_API = /api/v1/models)
# ─────────────────────────────────────────────

def _model_api_post(endpoint: str, payload: dict) -> dict | None:
    """
    POST tới {MODEL_API}/{endpoint}.
    Trả về response JSON hoặc None nếu lỗi.
    """
    url = MODEL_API.rstrip("/") + "/" + endpoint.lstrip("/")
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code in [200, 201]:
            return res.json()
        print(f"[MODEL_API] {endpoint} → {res.status_code}: {res.text}")
        return None
    except Exception as e:
        print(f"[MODEL_API] error calling {endpoint}: {e}")
        return None


def register_model(workspace_id: str, model_name: str, description: str = "", metadata: dict = None) -> dict | None:
    """
    POST /api/v1/models/register
    Tạo mới model trong registry.
    """
    return _model_api_post("register", {
        "workspace_id": workspace_id,
        "model_name":   model_name,
        "description":  description,
        "metadata":     metadata or {},
    })


def register_model_version(
    workspace_id: str,
    model_name: str,
    run_id: str,
    model_uri: str,
    description: str = "",
    metadata: dict = None,
) -> dict | None:
    """
    POST /api/v1/models/register-version
    Đăng ký version mới của model.
    """
    return _model_api_post("register-version", {
        "workspace_id": workspace_id,
        "model_name":   model_name,
        "run_id":       run_id,
        "model_uri":    model_uri,
        "description":  description,
        "metadata":     metadata or {},
    })


def define_model_metadata(
    workspace_id: str,
    model_name: str,
    version: str,
    metadata: dict = None,
) -> dict | None:
    """
    POST /api/v1/models/define-metadata
    Gán metadata cho một version.
    """
    return _model_api_post("define-metadata", {
        "model_name":   model_name,
        "workspace_id": workspace_id,
        "version":      str(version),
        "metadata":     metadata or {},
    })


def define_model_schema(
    workspace_id: str,
    model_name: str,
    version: str,
    feature_columns: list,
) -> dict | None:
    """
    POST /api/v1/models/define-schema
    Định nghĩa input schema cho model version.
    feature_columns: list tên cột (dtype mặc định float, required=True).
    """
    schema = [{"name": col, "dtype": "float", "required": True} for col in feature_columns]
    return _model_api_post("define-schema", {
        "model_name":   model_name,
        "workspace_id": workspace_id,
        "version":      str(version),
        "features":     schema,
    })


# =========================================================
# MLFLOW CONFIG
# =========================================================

class Config:
    MLFLOW_TRACKING_URI   = "http://10.168.6.78:32628"
    MLFLOW_S3_ENDPOINT    = "http://10.168.6.88:9000"
    AWS_ACCESS_KEY_ID     = "tutS09Tsa5VQvawZZPiP"
    AWS_SECRET_ACCESS_KEY = "KL4XSgspqTry9fTsgQwXq7zramPjqkghShNv4By6"
    MODEL_API             = "http://10.168.6.78:30080/api/v1/models"
    EXPERIMENT_NAME       = "iris_prod"  # người dùng định nghĩa hoặc là mặc định luôn
    ARTIFACT_PATH         = "model"      # Không thay đổi


def call_api(endpoint, payload):
    url = f"{Config.MODEL_API}/{endpoint}"
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code in [200, 201]:
            return res.json()
        return None
    except Exception as e:
        print(f"API error: {e}")
        return None


def setup_mlflow():
    os.environ["MLFLOW_S3_ENDPOINT_URL"]  = Config.MLFLOW_S3_ENDPOINT
    os.environ["AWS_ACCESS_KEY_ID"]       = Config.AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"]   = Config.AWS_SECRET_ACCESS_KEY
    mlflow.set_tracking_uri(Config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment(Config.EXPERIMENT_NAME)
    print(f"MLflow tracking URI: {Config.MLFLOW_TRACKING_URI}")


# =========================================================
# MODEL LOADER
# =========================================================

def load_model_universal(
    model_name: str,
    version: str,
    user: Optional[str] = None
) -> Tuple[object, str]:
    """Load model từ MLflow Registry mà không cần biết trước flavor."""
    full_name = f"{user}_{model_name}" if user else model_name
    model_uri = f"models:/{full_name}/{version}"
    loaders = [
        ("sklearn", mlflow.sklearn.load_model),
        ("xgboost", mlflow.xgboost.load_model),
        ("pytorch", mlflow.pytorch.load_model),
        ("pyfunc",  mlflow.pyfunc.load_model),
    ]
    for flavor, loader in loaders:
        try:
            model = loader(model_uri)
            print(f"Loaded model as [{flavor}]")
            return model, flavor
        except Exception:
            continue
    raise RuntimeError(f"Cannot load model from: {model_uri}")


def log_model_by_flavor(model, model_type: str, artifact_path: str):
    """Log model theo đúng MLflow flavor."""
    if model_type == "sklearn":
        return mlflow.sklearn.log_model(sk_model=model, artifact_path=artifact_path)
    if model_type == "xgboost":
        return mlflow.xgboost.log_model(xgb_model=model, artifact_path=artifact_path)
    if model_type == "pyfunc":
        return mlflow.pyfunc.log_model(python_model=model, artifact_path=artifact_path)
    raise ValueError(f"Unsupported model type: {model_type}")


def fill_information_for_portal_db(
    model_id: str,
    pipeline_ver_id: str,
    workspace_id: str,
    version: str,
    metrics: dict,
    params: dict,
    token: str
):
    url = f"{GATEWAY_URL}/inference/v1/inference-model-version/create"
    payload = {
        "modelId":           model_id,
        "pipelineVersionId": pipeline_ver_id,
        "workspaceId":       workspace_id,
        "versionTag":        str(version),
        "metrics":           metrics,
        "parameters":        params
    }
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}"
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        print("Fill portal db success")
        return response.json()
    except Exception as e:
        print(f"Fill portal db failed: {e}")
        return None


# =========================================================
# REGISTER MODEL — helpers
# =========================================================

def log_training_information(training_info: dict):
    parameters = training_info.get("parameters", {})
    metrics    = training_info.get("metrics", {})
    if parameters:
        mlflow.log_params(parameters)
    if metrics:
        mlflow.log_metrics(metrics)
    return parameters, metrics


def set_training_tags(training_info: dict, model_type: str):
    ignored_keys = {"parameters", "metrics"}
    for key, value in training_info.items():
        if key in ignored_keys:
            continue
        try:
            mlflow.set_tag(key, str(value))
        except Exception:
            pass
    mlflow.set_tag("model_type", model_type)


def log_model_pipeline(model, model_type: str, artifact_path: str):
    model_info = log_model_by_flavor(model=model, model_type=model_type, artifact_path=artifact_path)
    return model_info.model_uri


# ─── Payload builders ───

def build_register_model_payload(training_info: dict, model_type: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "description":  training_info.get("description", f"{model_type} model"),
        "metadata":     {**training_info, "model_type": model_type}
    }


def build_register_version_payload(training_info: dict, model_type: str, run_id: str, model_uri: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "run_id":       run_id,
        "model_uri":    model_uri,
        "description":  training_info.get("description", f"{model_type} version"),
        "metadata":     {**training_info, "model_type": model_type, "run_id": run_id}
    }


def build_metadata_payload(training_info: dict, model_type: str, version: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "version":      str(version),
        "metadata":     {**training_info, "model_type": model_type, "flavor": model_type}
    }


def build_schema_payload(training_info: dict, version: str):
    schema = [{"name": col, "dtype": "float", "required": True} for col in training_info.get("schema_define", [])]
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "version":      str(version),
        "features":     schema
    }


# ─── API callers ───

def register_model_api(training_info: dict, model_type: str):
    return call_api("register", build_register_model_payload(training_info, model_type))


def register_version_api(training_info: dict, model_type: str, run_id: str, model_uri: str):
    return call_api("register-version", build_register_version_payload(training_info, model_type, run_id, model_uri))


def define_metadata_api(training_info: dict, model_type: str, version: str):
    return call_api("define-metadata", build_metadata_payload(training_info, model_type, version))


def define_schema_api(training_info: dict, version: str):
    return call_api("define-schema", build_schema_payload(training_info, version))


# =========================================================
# VERIFY MODEL
# =========================================================

def verify_registered_model(workspace_id: str, model_name: str, version: str):
    full_name         = f"{workspace_id}_{model_name}"
    model_uri_registry = f"models:/{full_name}/{version}"
    loaded_model, loaded_type = load_model_universal(model_name=model_name, version=version, user=workspace_id)
    return {"loaded_model": loaded_model, "loaded_type": loaded_type, "model_uri_registry": model_uri_registry}


# =========================================================
# MAIN — registry_model
# =========================================================

def registry_model(model, training_info: dict, token: str, model_type: str = "auto"):
    """
    Orchestration: log model lên MLflow rồi đăng ký vào model registry API.
    Trả về (model, run_id, version).
    """
    workspace_id  = training_info["workspace_id"]
    model_name    = training_info["model_name"]
    full_name     = f"{workspace_id}_{model_name}"
    artifact_path = Config.ARTIFACT_PATH

    print("\n" + "=" * 60)
    print(f"REGISTRY MODEL PIPELINE [{model_type.upper()}]")
    print("=" * 60)

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Run ID: {run_id}")

        parameters, metrics = log_training_information(training_info)
        model_uri = log_model_pipeline(model=model, model_type=model_type, artifact_path=artifact_path)
        set_training_tags(training_info=training_info, model_type=model_type)

    # Register model
    print("\nRegistering model...")
    reg_res = register_model_api(training_info=training_info, model_type=model_type)
    if not reg_res or not reg_res.get("success"):
        print("Model may already exist")

    # Register version
    print("Registering version...")
    ver_res = register_version_api(training_info=training_info, model_type=model_type, run_id=run_id, model_uri=model_uri)
    if not ver_res or not ver_res.get("success"):
        print(f"Register version failed: {ver_res}")
        return None, None, None

    version = ver_res["data"]["version"]
    print(f"Version: {version}")

    # Define metadata & schema
    define_metadata_api(training_info=training_info, model_type=model_type, version=version)
    define_schema_api(training_info=training_info, version=version)

    # Verify
    print("\nVerifying model...")
    try:
        verify_info = verify_registered_model(workspace_id=workspace_id, model_name=model_name, version=version)
        print(f"Verification success — type: {verify_info['loaded_type']}")
    except Exception as e:
        print(f"Verification failed: {e}")

    # Update portal DB
    print("\nUpdate portal database...")
    fill_information_for_portal_db(
        model_id=training_info["model_id"],
        pipeline_ver_id=training_info["pipeline_ver_id"],
        workspace_id=workspace_id,
        version=version,
        metrics=metrics,
        params=parameters,
        token=token,
    )

    print("\n" + "=" * 60)
    print("REGISTRATION COMPLETE")
    print("=" * 60)
    print(f"Model Name : {full_name}")
    print(f"Model Type : {model_type}")
    print(f"Version    : {version}")
    print(f"Run ID     : {run_id}")
    print(f"models:/{full_name}/{version}")
    print("=" * 60)

    return model, run_id, version

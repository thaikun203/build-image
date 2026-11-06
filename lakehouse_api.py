"""
lakehouse_api.py
Tổng hợp tất cả hàm gọi API từ RegistryFeature, RegistryModel, FeatureData.
Import file này để dùng chung trong notebook/pipeline.

Usage:
    from lakehouse_api import get_bearer_token, registry_feature, register_model, ...
    from lakehouse_api import _rm_setup_mlflow, registry_model
"""

import http
import os
import re
import json
import math
import requests
import pandas as pd
from typing import Optional, Tuple

try:
    import mlflow
    import mlflow.pyfunc
except ImportError:
    mlflow = None

# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────

def get_bearer_token(login_url: str, username: str, password: str, client_secret: str, client_id: str = "data-platform") -> str:
    """Lấy access_token từ Keycloak với thông tin đăng nhập truyền vào trực tiếp."""
    url = f"{login_url}/realms/data-platform/protocol/openid-connect/token"
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "username":      username,
                "password":      password,
                "client_id":     client_id,
                "client_secret": client_secret,
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
    url = f"{os.environ.get('GATEWAY_URL', '')}/admin/v1/user/get-user-info"
    print(f"[get_user_info] POST {url}")
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
    url = f"{os.environ.get('GATEWAY_URL', '')}/lake/workspace-service/v1/catalog/spark-session"
    params = {
        "workspaceName":  os.environ.get("WORKSPACE_NAME"),
        "catalogName":    catalog_name,
        "schemaName":     schema_name,
        "organizationId": organization_id,
    }
    print(f"[_list_spark_session] GET {url}")
    print("[_list_spark_session] params:\n" + json.dumps(params, indent=2, ensure_ascii=False, default=str))
    response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)
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

def _pandas_to_trino_type(dtype) -> str:
    dtype_str = str(dtype)
    if dtype_str == "bool":
        return "boolean"
    elif dtype_str == "int8":
        return "tinyint"
    elif dtype_str == "int16":
        return "int"
    elif dtype_str in ("int32", "int64", "Int32"):
        return "int"
    elif dtype_str in ("Int64",):
        return "long"
    elif dtype_str == "float32":
        return "float"
    elif dtype_str == "float64":
        return "double"
    elif dtype_str in ("object", "string"):
        return "string"
    elif dtype_str.startswith("datetime64"):
        # A timezone-aware pandas dtype is represented as
        # ``datetime64[ns, <timezone>]``. Do not infer this from the string
        # length: plain ``datetime64[ns]`` is longer than 12 characters too
        # and was therefore incorrectly registered as ``timestamptz``.
        if getattr(dtype, "tz", None) is not None or "," in dtype_str:
            return "timestamptz"
        return "timestamp"
    elif dtype_str == "date":
        return "date"
    else:
        return "string"


def get_input_schema(df: pd.DataFrame, selected_cols: list) -> dict:
    """Tạo schemaDefinition theo Iceberg/Trino format: {col: trino_type, ...}. Tên cột luôn lowercase."""
    return {str(col).lower(): _pandas_to_trino_type(df[col].dtype) for col in selected_cols}


def _lower_schema_keys(schema: dict) -> dict:
    """Lowercase toàn bộ tên cột (key) trong schema dict trước khi gửi lên API."""
    return {str(k).lower(): v for k, v in (schema or {}).items()}


def _lower_output_definition(output_definition: dict) -> dict:
    """Lowercase tên cột target (metadata.target.column) trong outputDefinition trước khi gửi lên API."""
    if not output_definition:
        return output_definition or {}
    result = dict(output_definition)
    target = result.get("target")
    if isinstance(target, dict) and target.get("column"):
        target = dict(target)
        target["column"] = str(target["column"]).lower()
        result["target"] = target
    return result


def get_table_by_definition(featureGroupId: str, schemaDefinition: dict, token: str, outputDefinition: dict = None) -> dict:
    """
    POST /lake/inference-service/v1/feature-groups/table-by-definition
    Trả về thông tin bảng: trinoUrl, catalogName, schemaName, tableName, matched.

    schemaDefinition: schema của feature columns.
    outputDefinition: schema của target column (optional).
    API nhận full schema = schemaDefinition + outputDefinition để tìm đúng bảng.
    Tên cột luôn được lowercase trước khi gửi đi.
    """
    url = f"{os.environ.get('GATEWAY_URL', '')}/lake/inference-service/v1/feature-groups/table-by-definition"
    schemaDefinition = _lower_schema_keys(schemaDefinition)
    outputDefinition = _lower_schema_keys(outputDefinition)
    full_schema = {**schemaDefinition, **outputDefinition}
    body = {
        "featureGroupId":   featureGroupId,
        "schemaDefinition": full_schema,
    }
    print(f"[get_table_by_definition] POST {url}")
    print("[get_table_by_definition] body:\n" + json.dumps(body, indent=2, ensure_ascii=False, default=str))
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.post(url, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    return response.json().get("object", {})


def registry_feature(
    featureGroupId: str,
    pipelineVersionId: str,
    workspaceId: str,
    catalogName: str,
    schemaName: str,
    tableName: str,
    token: str,
    schemaDefinition: dict,
    outputDefinition: dict = None,
    trinoUrl: str = "",
    thoiGianCapNhat: str = "",
    tableId: str = "",
) -> dict:
    """
    POST /lake/inference-service/v1/feature-versions
    Đăng ký feature version lên lakehouse API.

    schemaDefinition: schema chỉ feature columns.
    outputDefinition: schema target column (optional, mặc định {}).
    Tên cột trong schemaDefinition và outputDefinition (target.column) luôn được lowercase trước khi gửi đi.
    trinoUrl: API chỉ nhận đúng 1 giá trị (không có field internal/external riêng) -- caller tự
    chọn URL nào cần gửi (theo TRINO_URL_TYPE lúc ghi) trước khi truyền vào đây.
    """
    url = f"{os.environ.get('GATEWAY_URL', '')}/lake/inference-service/v1/feature-versions"
    schemaDefinition = _lower_schema_keys(schemaDefinition)
    outputDefinition = _lower_output_definition(outputDefinition)
    body = {
        "featureGroupId":    featureGroupId,
        "pipelineVersionId": pipelineVersionId,
        "workspaceId":       workspaceId,
        "catalogName":       catalogName,
        "schemaName":        schemaName,
        "tableName":         tableName,
        "tableId":           tableId,
        "schemaDefinition":  schemaDefinition,
        "outputDefinition":  outputDefinition,
        "metadata":          "owner=data-team",
        "trinoUrl":          trinoUrl,
        "thoiGianCapNhat":   thoiGianCapNhat,
    }
    headers = {
        "Authorization": f"Bearer {token}",
    }
    try:
        print(f"[registry_feature] POST {url}")
        print("[registry_feature] body:\n" + json.dumps(body, indent=2, ensure_ascii=False, default=str))
        response = requests.post(url, headers=headers, json=body, timeout=30)
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

def _model_api_post(endpoint: str, payload: dict) -> Optional[dict]:
    """
    POST tới {MODEL_API}/{endpoint}.
    Trả về response JSON hoặc None nếu lỗi.
    """
    url = os.environ.get("MODEL_API", "http://10.168.6.107:8110/api/v1/models").rstrip("/") + "/" + endpoint.lstrip("/")
    try:
        print(f"[MODEL_API] POST {url}")
        res = requests.post(url, json=payload, timeout=10)
        print(f"[MODEL_API] {endpoint} → status: {res.status_code}")
        if res.status_code in [200, 201]:
            return res.json()
        print(f"[MODEL_API] response: {res.text}")
        return None
    except Exception as e:
        print(f"[MODEL_API] error calling {endpoint}: {e}")
        return None


def register_model(workspace_id: str, model_name: str, description: str = "", metadata: dict = None) -> Optional[dict]:
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
) -> Optional[dict]:
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
) -> Optional[dict]:
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
) -> Optional[dict]:
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
    MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "")
    MLFLOW_S3_ENDPOINT = os.environ.get("MLFLOW_S3_ENDPOINT", "")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    MODEL_API = os.environ.get("MODEL_API", "")
    EXPERIMENT_NAME = os.environ.get("EXPERIMENT_NAME", "")
    ARTIFACT_PATH = os.environ.get("ARTIFACT_PATH", "")



def _sanitize_for_json(obj):
    """Đệ quy thay NaN/Inf/numpy types thành giá trị JSON-safe."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return obj
    # Handles Python float AND numpy float32/float64/etc.
    try:
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return obj


def call_api(endpoint, payload):
    url = f"{Config.MODEL_API}/{endpoint}"
    try:
        sanitized = _sanitize_for_json(payload)
        print(f"[API] POST {url}")
        print("[API] body:\n" + json.dumps(sanitized, indent=2, ensure_ascii=False, default=str))
        res = requests.post(url, json=sanitized, timeout=10)
        print(f"[API] status: {res.status_code}")
        if res.status_code in [200, 201]:
            return res.json()
        print(f"[API] response: {res.text}")
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
    """Load model từ MLflow Registry bằng pyfunc (duy nhất)."""
    full_name = f"{user}_{model_name}" if user else model_name
    model_uri = f"models:/{full_name}/{version}"
    try:
        model = mlflow.pyfunc.load_model(model_uri)
        print(f"Loaded model as [pyfunc]")
        return model, "pyfunc"
    except Exception as e:
        raise RuntimeError(f"Cannot load model from {model_uri}: {e}")


def log_model_by_flavor(model, model_type: str, artifact_path: str):
    """Log model dưới dạng pyfunc, bất kể model_type là gì."""
    return mlflow.pyfunc.log_model(
        python_model=model,
        artifact_path=artifact_path
    )


def fill_information_for_portal_db(
    model_id: str,
    pipeline_ver_id: str,
    workspace_id: str,
    version: str,
    metrics: dict,
    params: dict,
    token: str
):
    url = f"{os.environ.get('GATEWAY_URL', '')}/lake/inference-service/v1/inference-model-version/create"
    valid_metrics = {
        k: v for k, v in (metrics or {}).items()
        if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
    }
    valid_params = {k: str(v) for k, v in (params or {}).items()}
    payload = {
        "modelId": model_id,
        "pipelineVersionId": pipeline_ver_id,
        "workspaceId": workspace_id,
        "versionTag": str(version),
        "metrics": valid_metrics,
        "parameters": valid_params
    }
    print(f"[fill_information_for_portal_db] POST {url}")
    print("[fill_information_for_portal_db] body:\n" + json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}"
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        print(f"[fill_information_for_portal_db] status: {response.status_code}")
        response.raise_for_status()
        print("Fill portal db success")
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Fill portal db failed: {e}")
        print(f"[fill_information_for_portal_db] response: {response.text}")
        return None
    except Exception as e:
        print(f"Fill portal db failed: {e}")
        return None


# =========================================================
# REGISTER MODEL — helpers
# =========================================================

# MLflow chỉ cho phép tên metric chứa chữ, số, '_', '-', '.', ' ', ':', '/'
_INVALID_METRIC_NAME_CHARS = re.compile(r"[^\w\-. :/]")


def _sanitize_metric_name(name: str) -> str:
    return _INVALID_METRIC_NAME_CHARS.sub("_", name)


def log_training_information(training_info: dict):
    parameters = training_info.get("parameters", {})
    metrics    = training_info.get("metrics", {})
    if parameters:
        mlflow.log_params(parameters)
    if metrics:
        valid_metrics = {
            _sanitize_metric_name(k): v for k, v in metrics.items()
            if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v)))
        }
        if valid_metrics:
            mlflow.log_metrics(valid_metrics)
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
    mlflow.set_tag("model_type", "pyfunc")


def log_model_pipeline(model, model_type: str, artifact_path: str):
    model_info = log_model_by_flavor(model=model, model_type="pyfunc", artifact_path=artifact_path)
    return model_info.model_uri


# ─── Payload builders ───

def build_register_model_payload(training_info: dict, model_type: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "description":  training_info.get("description", f"{model_type} model"),
        "metadata":     {**training_info, "model_type": "pyfunc"}
    }


def build_register_version_payload(training_info: dict, model_type: str, run_id: str, model_uri: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "run_id":       run_id,
        "model_uri":    model_uri,
        "description":  training_info.get("description", f"{model_type} version"),
        "metadata":     {**training_info, "model_type": "pyfunc", "run_id": run_id}
    }


def build_metadata_payload(training_info: dict, version: str):
    return {
        "workspace_id": training_info["workspace_id"],
        "model_name":   training_info["model_name"],
        "version":      str(version),
        "metadata":     training_info
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


def define_metadata_api(training_info: dict, version: str):
    payload = build_metadata_payload(training_info=training_info, version=version)
    return call_api("define-metadata", payload)


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
    print(f"REGISTRY MODEL PIPELINE [PYFUNC ONLY]")
    print("=" * 60)

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        print(f"Run ID: {run_id}")

        parameters, metrics = log_training_information(training_info)
        model_uri = log_model_pipeline(model=model, model_type="pyfunc", artifact_path=artifact_path)
        set_training_tags(training_info=training_info, model_type="pyfunc")

    # Register model
    print("\nRegistering model...")
    reg_res = register_model_api(training_info=training_info, model_type="pyfunc")
    if not reg_res or not reg_res.get("success"):
        print("Model may already exist")

    # Register version
    print("Registering version...")
    ver_res = register_version_api(training_info=training_info, model_type="pyfunc", run_id=run_id, model_uri=model_uri)
    if not ver_res or not ver_res.get("success"):
        print(f"Register version failed: {ver_res}")
        return None, None, None

    version = str(int(float(ver_res["data"]["version"])))
    print(f"Version: {version}")

    # Define metadata & schema
    define_metadata_api(training_info=training_info, version=version)
    # define_schema_api(training_info=training_info, version=version)

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
    print(f"Model Type : pyfunc")
    print(f"Version    : {version}")
    print(f"Run ID     : {run_id}")
    print(f"\nVersion URI  : models:/{full_name}/{version}")
    print(f"Champion URI : models:/{full_name}@champion")
    print("=" * 60)

    return model, run_id, version

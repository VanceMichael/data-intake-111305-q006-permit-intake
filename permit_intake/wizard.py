import json


def handle_get(handler, path):
    if not path.startswith(("/wizard/draft/", "/wizard/preview/")):
        return False
    application = path.rsplit("/", 1)[-1]
    with handler.server.state_lock:
        draft = handler.server.drafts.get(application)
        payload = {"application": application, **draft} if draft else {"error": "draft_missing"}
        handler.send_payload(200 if draft else 404, {"Content-Type": "application/json"}, json.dumps(payload, ensure_ascii=False).encode())
    return True


def handle_post(handler, path, fields):
    if path != "/wizard/save-step":
        return False
    with handler.server.state_lock:
        handler.server.requests.append({"method": "POST", "path": path, "fields": fields})
        draft = handler.server.drafts.get(fields.get("application"))
        schema = handler.server.scenario["routes"]["/wizard/schema"]["json"]
        try:
            incoming = json.loads(fields.get("fields", "{}"))
            revision = int(fields.get("revision", "-1"))
            if not isinstance(incoming, dict):
                raise ValueError("invalid field object")
        except (ValueError, TypeError):
            handler.send_payload(400, {"Content-Type": "application/json"}, b'{"error":"invalid_fields"}')
            return True
        if not draft:
            status, payload = 404, {"error": "draft_missing"}
        elif fields.get("csrf") != handler.server.scenario["portal"]["csrf"] or revision != draft["revision"]:
            status, payload = 409, {"error": "draft_changed", "revision": draft["revision"]}
        elif fields.get("step") not in schema["steps"]:
            status, payload = 422, {"error": "unknown_step"}
        else:
            step = schema["steps"][fields["step"]]
            merged = {**draft["fields"], **incoming}
            missing = [name for name in step["required"] if merged.get(name) in (None, "")]
            wrong_branch = any(merged.get(name) != value for name, value in step.get("when", {}).items())
            if missing or wrong_branch:
                status, payload = 422, {"error": "step_invalid", "missing": missing, "wrong_branch": wrong_branch}
            else:
                draft["fields"] = merged
                draft["revision"] += 1
                draft.setdefault("validated_steps", {})[fields["step"]] = draft["revision"]
                status, payload = 200, {"revision": draft["revision"], "validated_step": fields["step"]}
        handler.send_payload(status, {"Content-Type": "application/json"}, json.dumps(payload, ensure_ascii=False).encode())
    return True

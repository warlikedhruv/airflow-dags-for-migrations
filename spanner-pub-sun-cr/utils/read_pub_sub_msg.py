import base64


def read(request):
    data_keyword = "data"
    data = None
    if not request:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return None

    if not isinstance(request, dict) or "message" not in request:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return  None

    pubsub_message = request["message"]

    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        data = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
    return data

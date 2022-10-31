import base64


def read(request):
    try:
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
        print("DEBUG PUBSUB", pubsub_message)
        if isinstance(pubsub_message, dict) and "attributes" in pubsub_message:
            if "interface_name" in pubsub_message["attributes"]:
                if pubsub_message["attributes"]["interface_name"] == "oracle":
                    return True

            # data = base64.b64decode(pubsub_message["attributes"]).decode("utf-8").strip()
        return False
    except Exception as e:
        print("exception in read msg ", str(e))
    return False 
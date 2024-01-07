from nested import get_value_from_nested_json

message = {

    "eventId": "7ec7fd93-9473-4458-a0fa-abbf9529138f",
    # ... other fields ...
    "payload": {
        "recId": "1266740",
        "CustomerCode": None,
        "Mnemonic": "BK1266740",
        "ARRAY_ShortName": [
            {"ShortName": "NTAWIYOBERA ESTHER"},
            {"test": "Gentil"}
        ],
}

}

path_to_shortname = "payload.ARRAY_ShortName.test"


extracted_short_names = get_value_from_nested_json(message, path_to_shortname)
print("Extracted  Short Names:", extracted_short_names)
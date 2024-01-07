from nested import get_value_from_nested_json

sample_message = {
    "payload": {
        "ARRAY_Address": [
            {
                "SUB_ARRAY_Address": [
                    {
                        "Address": "KINIGI"
                    }
                ]
            }
        ],
        # ... other fields ...
    }
}

# Test extraction of Address
path_to_address = "payload.ARRAY_Address.SUB_ARRAY_Address.Address"
extracted_addresses = get_value_from_nested_json(sample_message, path_to_address)
print("Extracted Addresses:", extracted_addresses)


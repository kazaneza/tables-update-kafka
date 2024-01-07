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

def get_addresses_from_nested_json(data):

    addresses = []
    if 'payload' in data:
        print("Payload found")
        if 'ARRAY_Address' in data['payload']:
            print("ARRAY_Address found")
            for address_entry in data['payload']['ARRAY_Address']:
                if 'SUB_ARRAY_Address' in address_entry:
                    print("SUB_ARRAY_Address found")
                    for sub_address_entry in address_entry['SUB_ARRAY_Address']:
                        if 'Address' in sub_address_entry:
                            print("Address found:", sub_address_entry['Address'])
                            addresses.append(sub_address_entry['Address'])
                        else:
                            print("Address not found in sub_address_entry")
                else:
                    print("SUB_ARRAY_Address not found in address_entry")
        else:
            print("ARRAY_Address not found in payload")
    else:
        print("Payload not found in data")
    return addresses

# Using the custom function
extracted_addresses = get_addresses_from_nested_json(sample_message)
print("Extracted Addresses:", extracted_addresses)

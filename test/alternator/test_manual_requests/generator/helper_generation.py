###### Test generation helpers
###### put in test_manual_requests.py and uncomment
'''
def store_response(name, test, test_response):
    resp = "AWS" if "AWS" in name else "ALT"
    test[resp] = test_response
    with open(Path(__file__).with_suffix("") / (name+".resp.yaml"), "a+") as stream:
        yaml.safe_dump([ test ], stream, sort_keys=False)

def test_generated_ALT(dynamodb, test_table, test_filedata):
    if is_aws(dynamodb):
        return
    body = test_filedata["body"]
    body = (body if isinstance(body, str) else json.dumps(body)).replace("__TABLE__", test_table.name).replace("__ATTR__", "p")
    req = get_signed_request(dynamodb, test_filedata["request"], body)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    store_response("test_generated_ALT", test_filedata, response.text)

def test_generated_AWS(dynamodb, test_table, test_filedata):
    if not is_aws(dynamodb):
        return
    body = test_filedata["body"]
    body = (body if isinstance(body, str) else json.dumps(body)).replace("__TABLE__", test_table.name).replace("__ATTR__", "p")
    req = get_signed_request(dynamodb, test_filedata["request"], body)
    response = requests.post(req.url, headers=req.headers, data=req.body, verify=False)
    store_response("test_generated_AWS", test_filedata, response.text)
'''
##############################
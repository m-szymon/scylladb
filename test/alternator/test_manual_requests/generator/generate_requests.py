import yaml
import json

from pathlib import Path

class DevTestGenerator:
    class MemberReference(object):
        def __init__(self, obj, member, ref_name=None):
            self.obj = obj
            self.mem = member
            self.name = member if ref_name is None else ref_name
        def ref(self):
            return self.obj[self.mem]
        def assign(self, val):
            self.obj[self.mem] = val
    class KeyReference(object):
        def __init__(self, obj, key, ref_name=None):
            self.obj = obj
            self.key = key
            self.name = key if ref_name is None else ref_name
        def ref(self):
            return self.key
        def assign(self, val):
            self.obj[val] = self.obj[self.key]
            del self.obj[self.key]
            self.key = val

    def create_valid(shapes, name, v=0):
        if name == "TableArn" or name == "TableName":
            return "__TABLE__"
        if name == "AttributeName":
            return "__ATTR__"
        if name == "ResourceArnString":
            return "arn:test"
        sh = shapes[name]
        t = sh["type"]
        if t == "string":
            if "enum" in sh:
                return sh["enum"][0]
            return "_TEST_"  
        elif t == "structure":
            ret = {}
            if "required" in sh:
                for mem in sh["required"]:
                    ret[mem] = DevTestGenerator.create_valid(shapes, sh["members"][mem]["shape"])
            return ret
        elif t == "map":
            ret = {}
            if "min" in sh:
                for id in range(sh["min"]):
                    ret[DevTestGenerator.create_valid(shapes, sh["key"]["shape"], id)] = DevTestGenerator.create_valid(shapes, sh["value"]["shape"], id)
            return ret
        elif t == "list":
            ret = []
            if "min" in sh:
                for id in range(sh["min"]):
                    ret.append(DevTestGenerator.create_valid(shapes, sh["member"]["shape"], id))
            return ret
        return None
    def try_call(ref, value, callback, call_name):
        obj = ref.ref()
        try:
            ref.assign(value)
        except:
            return
        callback(call_name)
        ref.assign(obj)
    def create_invalid(shapes, name, ref, callback, max_level=10):
        if(max_level == 0):
            return
        def output(call_name):
            callback(ref.name, name, call_name)
        obj = ref.ref()
        sh = shapes[name]
        #print(name)
        if name == "AttributeValue" and max_level > 2:
            max_level = 2
        t = sh["type"]
        DevTestGenerator.try_call(ref, None, output, "unexpected_null")

        if t != "string":
            DevTestGenerator.try_call(ref, "invalid", output, "unexpected_string")

        if t != "structure" and t != "map":
            DevTestGenerator.try_call(ref, {}, output, "unexpected_object")

        if t != "list":
            DevTestGenerator.try_call(ref, [], output, "unexpected_list")

        if t == "string":
            if name == "TableArn":
                DevTestGenerator.try_call(ref, "+@", output, "invalid_table_name")
            if name == "AttributeName":
                DevTestGenerator.try_call(ref, "$*", output, "invalid_attr_name")
            if name == "ResourceArnString":
                DevTestGenerator.try_call(ref, "!%", output, "invalid_resource_name")
            if "enum" in sh:
                DevTestGenerator.try_call(ref, "invalid", output, "invalid_enum")
            if "min" in sh and sh["min"] > 0:
                DevTestGenerator.try_call(ref, "", output, "empty_string")
            if "pattern" in sh:
                DevTestGenerator.try_call(ref, "!@#$%^&*", output, "forbidden_string")
        elif t == "structure":
            for mem in sh["members"]:
                required = mem in obj
                if not required:
                    obj[mem] = DevTestGenerator.create_valid(shapes, sh["members"][mem]["shape"])
                else:
                    tmp = obj[mem]
                    del obj[mem]
                    callback(ref.name, name, mem+"_missing")
                    obj[mem] = None
                    callback(ref.name, name, mem+"_null")
                    obj[mem] = tmp

                new_ref = DevTestGenerator.MemberReference(obj, mem)
                DevTestGenerator.create_invalid(shapes, sh["members"][mem]["shape"], new_ref, callback, max_level-1)
                if not required:
                    del obj[mem]
        elif t == "map":
            empty = (len(obj) == 0)
            if empty:
                key = DevTestGenerator.create_valid(shapes, sh["key"]["shape"])
                value = DevTestGenerator.create_valid(shapes, sh["value"]["shape"])
                obj[key] = value
            else:
                key = next(iter(obj))
                tmp = obj[key]
                del obj[key]
                callback(ref.name, name, "missing_value")
                obj[key] = None
                callback(ref.name, name, "null_value")
                obj[key] = tmp
            new_ref = DevTestGenerator.MemberReference(obj, key, ref.name)
            DevTestGenerator.create_invalid(shapes, sh["value"]["shape"], new_ref, callback, max_level-1)
            new_key_ref = DevTestGenerator.KeyReference(obj, key, ref.name)
            DevTestGenerator.create_invalid(shapes, sh["key"]["shape"], new_key_ref, callback, max_level-1)
            if empty:
                del obj[key]
        elif t == "list":
            empty = (len(obj) == 0)
            if empty:
                obj.append(DevTestGenerator.create_valid(shapes, sh["member"]["shape"]))
            else:
                tmp = obj[0]
                del obj[0]
                callback(ref.name, name, "missing_value")
                obj.append(None)
                callback(ref.name, name, "null_value")
                obj[0] = tmp
            new_ref = DevTestGenerator.MemberReference(obj, 0, ref.name)
            DevTestGenerator.create_invalid(shapes, sh["member"]["shape"], new_ref, callback, max_level-1)
            if empty:
                del obj[0]

    def generate(path, unsupported_path="test_automated_unsupported_yet.yaml"):
        supported = ["CreateTable", "DescribeTable", "DeleteTable", "UpdateTable", "PutItem", "UpdateItem",
                     "GetItem", "DeleteItem", "ListTables", "Scan", "DescribeEndpoints", "BatchWriteItem",
                     "BatchGetItem","Query", "TagResource", "UntagResource", "ListTagsOfResource", "UpdateTimeToLive",
                     "DescribeTimeToLive", "ListStreams", "DescribeStream", "GetShardIterator", "GetRecords", "DescribeContinuousBackups"]
        tests = []
        unsupported = []
        with open("generator/service-2.json") as stream:
            service = json.load(stream)

            for op in service["operations"]:
                if op not in supported:
                    un_test = {"id": op, "request": op, "body": "{}"}
                    unsupported.append(un_test)

            for op in service["operations"]:
                if op not in supported:
                    continue
                obj = [ DevTestGenerator.create_valid(service["shapes"], service["operations"][op]["input"]["shape"]) ]
                ref = DevTestGenerator.MemberReference(obj, 0, "body")
                print(f"Valid: {ref.ref()}")
                def out(refname, typename, msg):
                    test = {"id": f"{op}-{refname}-{typename}-{msg}", "request": op, "body": json.dumps(ref.ref())}
                    tests.append(test)
                    print(f"Invalid: {ref.ref()}")
                DevTestGenerator.create_invalid(service["shapes"], service["operations"][op]["input"]["shape"], ref, out, 8)
        with open(path, "w") as stream:
            yaml.safe_dump(tests, stream, sort_keys=False)
        if unsupported_path is not None:
            with open(unsupported_path, "w") as stream:
                yaml.safe_dump(unsupported, stream, sort_keys=False)

test_invalid = Path("test_automated_invalid_payload.yaml")
if not test_invalid.exists():
    with open(test_invalid, "w") as stream:
        yaml.safe_dump([], stream, sort_keys=False)

test_valid = Path("test_automated_valid_payload.yaml")
if not test_valid.exists():
    with open(test_valid, "w") as stream:
        yaml.safe_dump([], stream, sort_keys=False)

service = Path("generator/service-2.json")
if not service.exists():
    import urllib.request
    with urllib.request.urlopen('https://raw.githubusercontent.com/boto/botocore/refs/heads/develop/botocore/data/dynamodb/2012-08-10/service-2.json') as f:
        html = f.read().decode('utf-8')
        with open(service, "w") as stream:
            stream.write(html)

generated = Path("generator/generated.yaml")
if not generated.exists():
    DevTestGenerator.generate(generated)

aws_history = Path("generator/AWS_history.yaml")
if not aws_history.exists():
    with open(aws_history, "w") as stream:
        yaml.safe_dump({}, stream, sort_keys=False)

aws_pending = Path("test_generated_AWS.yaml")
aws_reply = Path("test_generated_AWS.resp.yaml")

if aws_reply.exists():
    with open(aws_reply) as stream:
        aws_reply_data = yaml.safe_load(stream)
        if aws_reply_data is not None:
            aws_data = {}
            with open(aws_history) as stream:
                aws_data = yaml.safe_load(stream)
                for t in aws_reply_data:
                    if not t["request"] in aws_data:
                        aws_data[t["request"]] = {}
                    aws_data[t["request"]][t["body"]] = t["AWS"]
            with open(aws_history, "w") as stream:
                yaml.safe_dump(aws_data, stream)
    with open(aws_reply, "w") as stream:
        pass

alt_pending = Path("test_generated_ALT.yaml")
alt_reply = Path("test_generated_ALT.resp.yaml")
with open(aws_history) as stream:
    aws_data = yaml.safe_load(stream)
    aws_ready_tests = []
    pending_tests = []
    with open(generated) as stream:
        gen_data = yaml.safe_load(stream)
        for gen in gen_data:
            if not gen["request"] in aws_data:
                aws_data[gen["request"]] = {}
            if gen["body"] in aws_data[gen["request"]]:
                gen["AWS"] = aws_data[gen["request"]][gen["body"]]
                aws_ready_tests.append(gen)
            else:
                pending_tests.append(gen)
    with open(aws_pending, "w") as stream:
        yaml.safe_dump(pending_tests, stream, sort_keys=False)
    with open(alt_pending, "w") as stream:
        yaml.safe_dump(aws_ready_tests, stream, sort_keys=False)

invalid = Path("generator/full_invalid_payload.yaml")
valid = Path("generator/full_valid_payload.yaml")
other = Path("generator/full_other_errors.yaml")
if alt_reply.exists():
    with open(alt_reply) as stream:
        test_data = yaml.safe_load(stream)
        invalid_data = []
        valid_data = []
        test_invalid_data = []
        test_valid_data = []
        other_data = []
        if test_data is not None:
            for test in test_data:
                if ("ValidationException" in test["AWS"] or "SerializationException" in test["AWS"]):
                    invalid_data.append(test)
                    test_invalid_data.append({"id": test["id"], "request": test["request"], "body": test["body"]})
                else:
                    r = json.loads(test["AWS"])
                    if '__type' in r:
                        other_data.append(test)
                    else:
                        valid_data.append(test)
                        test_valid_data.append({"id": test["id"], "request": test["request"], "body": test["body"]})
            with open(invalid, "w") as stream:
                yaml.safe_dump(invalid_data, stream, sort_keys=False)
            with open(test_invalid, "w") as stream:
                yaml.safe_dump(test_invalid_data, stream, sort_keys=False)
            with open(valid, "w") as stream:
                yaml.safe_dump(valid_data, stream, sort_keys=False)
            with open(test_valid, "w") as stream:
                yaml.safe_dump(test_valid_data, stream, sort_keys=False)
            with open(other, "w") as stream:
                yaml.safe_dump(other_data, stream, sort_keys=False)
    with open(alt_reply, "w") as stream:
        pass

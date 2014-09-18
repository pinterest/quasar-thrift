namespace java com.pinterest.example.thrift

enum ExampleEnum {
    VALUE_ONE,
    VALUE_TWO,
    VALUE_THREE
}

const i64 INT_CONST = 36;

struct ExampleRequest {
    // Basic Types
    1: optional bool optBool;
    2: required byte reqByte;
    3: optional i16 optI16;
    4: required i32 reqI32;
    5: optional i64 optI64 = INT_CONST;
    6: required double reqDouble;
    7: optional string optString = "foo";

    // Containers
    8: optional list<string> listOfStrings;
    9: optional set<string> setOfStrings;
    10: optional map<string, i32> mapStringToI32;

    // Enum
    11: optional ExampleEnum exampleEnum;
}

struct ExampleResponse {
    1: optional string message;
}

exception ExampleException {
    1: string message;
}

service ExampleService {
    ExampleResponse example(1: ExampleRequest request) throws (1:ExampleException exc);
}
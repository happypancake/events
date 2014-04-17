package eventstore

import "encoding/json"

type Envelope interface {
	Payload() (contract string, data []byte)
}

func New(contract string, data []byte) Envelope {
	return &envelope{contract, data}
}

type Message interface {
	Contract() string
}

type envelope struct {
	Contract string
	Data     []byte
}

func (e *envelope) Payload() (string, []byte) {
	return e.Contract, e.Data
}

func Marshal(m Message) Envelope {
	data, _ := json.Marshal(m)
	return &envelope{m.Contract(), data}
}
func MarshalDynamic(contract string, dynamic interface{}) Envelope {
	data, _ := json.Marshal(dynamic)
	return &envelope{contract, data}
}

type Dict map[string]interface{}

func UnmarshalDynamic(data []byte) Dict {
	// TODO: use contract to determine json/bson serialization
	//_, data := e.Payload()
	var dict Dict
	json.Unmarshal(data, &dict)
	return dict
}
func Unmarshal(data []byte, msg Message) {
	//_, data := e.Payload()
	json.Unmarshal(data, msg)
}
func (d Dict) StrOrNil(key string) string {
	if val, ok := d[key]; !ok {
		return ""
	} else if res, ok := val.(string); ok {
		return res
	} else {
		// TODO: handle int2Str here
		return ""
	}
}

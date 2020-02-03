package encoding

import (
	"bytes"
	"time"
	"strconv"
	"fmt"
	"encoding/json"
	"os"
	"log"
	"mom-test/structs"

	"github.com/linkedin/goavro"
)

const loginEventAvroSchema = `{"type": "record", "name": "LoginEvent", "fields": [{"name": "TheTime", "type": "string"}, {"name": "ScareMe", "type": "string"}, {"name": "Binaryfile", "type": "bytes"}]}`

// EncodeAvro encodes a message into the avro OCF (Avro object) format invented by the Apache Foundation
// returns the encoded message as byte array and the timeduration in Nanoseconds of encoding the message
func EncodeAvro(conProdID int, messageID int, scareMe string, binary []byte) ([]byte, int64){
	// println("avro compression starting....")
    var b bytes.Buffer
    // foo := bufio.NewWriter(&b)

	codec, err := goavro.NewCodec(loginEventAvroSchema)
	if err != nil {
		panic(err)
	}

	var values []map[string] interface{}
	theTime := strconv.Itoa(int(time.Now().UnixNano()))
	// values = append(values, m)
	// values = append(values, map[string] interface{}{"TheTime": "643573245", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	// values = append(values, map[string] interface{}{"TheTime": "657987654", "ScareMe": "scareMe", "Binaryfile": []byte{0, 1, 2, 4, 5}})
	values = append(values, map[string] interface{}{"TheTime": theTime, "ScareMe": scareMe, "Binaryfile": binary})

	// f, err := os.Create("event.avro")
	// if err != nil {
	// 	panic(err)
	// }
	// startTime = strconv.Itoa(int(time.Now().UnixNano()))	//--> important to use this command twice, because of accourate time measurement !

	startTime := time.Now().UnixNano()

	ocfw, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:     &b,
		Codec: codec,
	})
	if err != nil {
		panic(err)
	}
	// println(values)
	// println("Appending bytes")
	ocfw.Append(values)
	// if err = ocfw.Append(values); err != nil {
	// 	panic(err)
	// }
	bytearray:= b.Bytes()
	// endTime := time.Now().UnixNano()
	// duration:= endTime - startTime

	// control printout
	// println(b.Cap())
	// fmt.Printf("Time: %s \n", jsonMsg.TheTime)
	// fmt.Printf("ScareMe: %s \n", jsonMsg.ScareMe)
	// fmt.Printf("Binary: %s \n", jsonMsg.Binaryfile)

	// println("avro compression finished!!!")

	return bytearray, startTime
}

// DecodeAvro decodes a message the specific format 'Myinfo' included in this testsystem
// returns the decoded message as byte array and the timeduration in Nanoseconds of encoding the message
func DecodeAvro(conProdID int, messageID int, message []byte) (structs.MyInfo, int64){
	var output structs.MyInfo
	b := bytes.NewBuffer(message)
	
	// decode this shit to verify
	ocfr, err := goavro.NewOCFReader(b)
	if err != nil{
		panic(err)
	}

	var decoded interface{}
	// println(ocfr.Scan()) //--> if true, messages available
	startTime := time.Now().UnixNano()

	for ocfr.Scan(){
			decoded, err = ocfr.Read()
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
			// fmt.Println(decoded)
			// fmt.Printf("Test mit Printf: %v \n", decoded)
	}

	jsontest, err := json.Marshal(decoded)
	if err != nil{
		log.Fatal(err)
	}
	// println(string(jsontest))
	json.Unmarshal(jsontest, &output)

	// endTime := time.Now().UnixNano()
	// duration:= endTime - startTime

	// fmt.Printf("Myinfo1: %s \n", output)
	// fmt.Printf("Time: %s \n", output.TheTime)
	// fmt.Printf("ScareMe: %s \n", output.ScareMe)
	// fmt.Printf("Binary: %s \n", output.Binaryfile)

	return output, startTime
}
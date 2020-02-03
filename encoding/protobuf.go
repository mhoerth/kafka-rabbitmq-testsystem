package encoding

import (
	"strconv"
	"time"
	"log"
	"mom-test/structs"

	"github.com/golang/protobuf/proto"
)
// EncodeProto encodes a message into the protobuf format invented by Google
// returns the encoded message as byte array and the timeduration in Nanoseconds of encoding the message
func EncodeProto(conProdID int, messageID int, scareMe string, binary []byte) ([]byte, int64){
	// get current time
	getTime := strconv.Itoa(int(time.Now().UnixNano()))

	// define object content
	object := &TestProto{
        TheTime: getTime,
		ScareMe:  scareMe,
		BinaryFile: binary,
    }

	startTime := time.Now().UnixNano()
	// marshal content like json format
    data, err := proto.Marshal(object)
    if err != nil {
        log.Fatal("marshaling error: ", err)
    }

	// endTime := time.Now().UnixNano()
	// duration:= endTime - startTime
	// durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
	// encodingTime[conProdID][messageID] = strconv.FormatFloat(durationMs, 'f', 6, 64)

  // printing out our raw protobuf object
    // fmt.Println(data)

	return data, startTime
}

// DecodeProto decodes a message the specific format 'Myinfo' included in this testsystem
// returns the decoded message as 'Myinfo'-object and the timeduration in Nanoseconds of encoding the message
func DecodeProto(conProdID int, messageID int, message []byte) (structs.MyInfo, int64){
  // byte array into an object which can be modified and used
  object := &TestProto{}

  startTime := time.Now().UnixNano()

  err := proto.Unmarshal(message, object)
  if err != nil {
	  log.Fatal("unmarshaling error: ", err)
}

// protobuf to json
var jsonMsg structs.MyInfo

jsonMsg.TheTime = object.GetTheTime()
jsonMsg.ScareMe = object.GetScareMe()
jsonMsg.Binaryfile = object.GetBinaryFile()

// endTime := time.Now().UnixNano()
// duration:= endTime - startTime
// durationMs := float64(duration) / float64(1000000) //Nanosekunden in Milisekunden
// decodingTime[conProdID][messageID] = strconv.FormatFloat(durationMs, 'f', 6, 64)

	// print out
	// fmt.Printf("Time: %s \n", jsonMsg.TheTime)
	// fmt.Printf("ScareMe: %s \n", jsonMsg.ScareMe)
	// fmt.Printf("Binary: %s \n", jsonMsg.Binaryfile)

	return jsonMsg, startTime
}
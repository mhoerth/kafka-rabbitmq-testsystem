package structs

// MyInfo is a struct provided for testing purposes
// Field: TheTime type string containing the timestamp of a message
// Field: ScareMe type string containing a funny information of a message
// Field: Binaryfile type []byte containing a large information of a message
type MyInfo struct {
	TheTime    string `json:"theTime"`
	ScareMe    string `json:"scareme"`
	Binaryfile []byte `json:"binaryfile"`
}

// Csv is a struct provided for writing the testresults into a csv file
// Field: Testsystem type string containing the Testsystem Name (to identify the corresponding system after the test in the testoutput)
// Field: Messages type int containing the amount of messages used in one test interation
// Field: CountProdCon type int containing the amount of increasing hops (consumer and Producer instances)
// Field: MessageSize type string containing the Name of the used binary file for one testinteration
// Field: CompressionType type string containing the type of compression (json, avro, protobuf)
// Field: SendTime type string containing the timearray for the time needed to send every message
// Field: ConSendTime type string containing the timearray for the time needed to receive and send every message (producer/consumer instances)
// Field: ConsumeTime type string containing the timearray for the time needed to consume every message
// Field: EncodingTime type string containing the timearray for the time needed to encode every message
// Field: DecodingTime type string containing the timearray for the time needed to decode every message
// Field: CompleteTime type float containing the time need to consume all messages
type Csv struct{
	Interation int
	Testsystem string
	Messages int
	CountProdCon int
	MessageSize string
	CompressionType string
	SendTime [1000000]string
	SendTimeStamps [1000000]string
	ConSendTime [9][1000000]string	//8 possible consumer+producer
	ConsumeTimeStamps [1000000]string
	ConsumeTime [1000000]string
	EncodingTime [9][1000000]string	//encodingTime[0][] is reserved for 'normal' producer
	DecodingTime [9][1000000]string //encodingTime[0][] is reserved for 'normal' consumer
	RoundTripTime[1000000]string
	CompleteTime float64
	Filesize int64
	MsDelay int
}
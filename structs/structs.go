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
package output

import (
	"os"
	"regexp"
	"strings"
	"strconv"
	
	"../structs"

)

// Csv provide the creation of a csv file out of a Csv struct
func Csv(csvStruct structs.Csv){
	// write file
	// get messageSize (digits) out of the filename
	re:=regexp.MustCompile("[0-9]+")
	csvStruct.MessageSize = strings.Join(re.FindAllString(csvStruct.MessageSize, -1), "")
	
	// create outputfile with set parameters to identify the test after programm execution
	f, err := os.Create(strconv.Itoa(csvStruct.Interation) + "_" + csvStruct.Testsystem + "_" + strconv.Itoa(csvStruct.CountProdCon) + "_" + csvStruct.CompressionType + "_" + strconv.Itoa(csvStruct.Messages) + "_" + csvStruct.MessageSize + "Kibi" + ".csv")
	if err != nil{
		panic(err)
	}
	defer f.Close()

	f.WriteString("Messagenumber;")
	f.WriteString("ProducerSendTime;")
	f.WriteString("ProducerEncodingTime;")
	if csvStruct.CountProdCon > 0{
		f.WriteString("Consumer&ProducerSendTime;")
		f.WriteString("Consumer&ProducerEncodeTime;")
		f.WriteString("Consumer&ProducerDecodeTime;")	
	}
	f.WriteString("ConsumerTime;")
	f.WriteString("ConsumerDecodingTime;")
	f.WriteString("\n")

	for i:=0; i < csvStruct.Messages; i++{
		f.WriteString(strconv.Itoa(i) + " ;") //Messagenumber
		f.WriteString(csvStruct.SendTime[i] + ";")
		f.WriteString(csvStruct.EncodingTime[0][i] + ";")
		for cp:=1; cp <= csvStruct.CountProdCon; cp++{
			// f.WriteString("Consumer&ProducerSendTime: ")
			// f.WriteString(";")
			f.WriteString(csvStruct.ConSendTime[cp][i] + ";")
			f.WriteString(csvStruct.EncodingTime[cp][i] + ";")
			f.WriteString(csvStruct.DecodingTime[cp][i])
			if(cp < csvStruct.CountProdCon){
				f.WriteString("; \n"  + strconv.Itoa(i) +  "; ;")
			}else{
				f.WriteString(";")
			}
		}
		// f.WriteString("ConsumerTime: ")
		f.WriteString(csvStruct.ConsumeTime[i] + ";")
		f.WriteString(csvStruct.DecodingTime[0][i])
		f.WriteString("; \n")
	}

	f.WriteString("CompleteTimeDuration;")
	f.WriteString(strconv.FormatFloat(csvStruct.CompleteTime, 'f', 6, 64))
	f.WriteString("; \n")
	f.WriteString("Filesize;")
	f.WriteString(strconv.FormatInt(csvStruct.Filesize, 10))
	f.WriteString("; \n")
}
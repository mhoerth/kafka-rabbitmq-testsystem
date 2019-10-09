package output

import (
	"os"
	"regexp"
	"strings"
	"strconv"
	
	"../structs"

)

// Csv provide the creation of a csv file out of a Csv struct
func Csv(csvStruct structs.Csv, cpuStart []byte, cpuStop []byte){
	// write file
	// get messageSize (digits) out of the filename
	re:=regexp.MustCompile("[0-9]+")
	csvStruct.MessageSize = strings.Join(re.FindAllString(csvStruct.MessageSize, -1), "")
	
	// create outputfile with set parameters to identify the test after programm execution
	f, err := os.Create(strconv.Itoa(csvStruct.Interation) + "_" + csvStruct.Testsystem + "_" + strconv.Itoa(csvStruct.CountProdCon) + "_" + csvStruct.CompressionType + "_" + strconv.Itoa(csvStruct.Messages) + "_" + csvStruct.MessageSize + "Kibi" + "_" + strconv.Itoa(csvStruct.MsDelay) + "(ms)Delay" + ".csv")
	if err != nil{
		panic(err)
	}
	defer f.Close()

	f.WriteString("Messagenumber;")
	f.WriteString("ProducerEncodingTime;")
	f.WriteString("ProducerSendTime;")
	if csvStruct.CountProdCon > 0{
		f.WriteString("Consumer&ProducerMessageTransmitTime;")
		f.WriteString("Consumer&ProducerDecodeTime;")	
		f.WriteString("Consumer&ProducerEncodeTime;")
		f.WriteString("Consumer&ProducerSendTime;")
	}
	f.WriteString("LastConsumerMessageTransmitTime;")
	f.WriteString("ConsumerDecodingTime;")
	f.WriteString("RoundTripTime;")
	f.WriteString("\n")

	for i:=0; i < csvStruct.Messages; i++{
		f.WriteString(strconv.Itoa(i) + " ;") //Messagenumber
		f.WriteString(csvStruct.EncodingTime[0][i] + ";")
		f.WriteString(csvStruct.SendTime[0][i] + ";")
		for cp:=1; cp <= csvStruct.CountProdCon; cp++{
			// f.WriteString("Consumer&ProducerSendTime: ")
			// f.WriteString(";")
			f.WriteString(csvStruct.ConsumeTime[cp -1][i] + ";")		//cp -1 because first prodcon writes its values into the field [0][i]
			f.WriteString(csvStruct.DecodingTime[cp][i] + ";")
			f.WriteString(csvStruct.EncodingTime[cp][i] + ";")
			f.WriteString(csvStruct.SendTime[cp][i])
			if(cp < csvStruct.CountProdCon){
				f.WriteString("; \n"  + strconv.Itoa(i) +  "; ; ;")
			}else{
				f.WriteString(";")
			}
		}
		// f.WriteString("ConsumerTime: ")
		f.WriteString(csvStruct.ConsumeTime[csvStruct.CountProdCon][i] + ";")
		f.WriteString(csvStruct.DecodingTime[csvStruct.CountProdCon][i] + ";")
		f.WriteString(csvStruct.RoundTripTime[i])
		f.WriteString("; \n")
	}

	f.WriteString("CompleteTimeDuration;")
	f.WriteString(strconv.FormatFloat(csvStruct.CompleteTime, 'f', 6, 64))
	f.WriteString("; \n")
	f.WriteString("Filesize(byte);")
	f.WriteString(strconv.FormatInt(csvStruct.Filesize, 10) + "\n")

	// cmd := exec.Command("cat", "/proc/stat |grep cpu")
    // out, err := cmd.CombinedOutput()
    // if err != nil {
    //     panic(err)
    // }
	// println(string(out))
	
	// out, err := exec.Command("cat", "/proc/stat |grep cpu").Output()
	// out, err := exec.Command("bash", "-c", "cat /proc/stat |grep cpu").Output()
    // if err != nil {
    //     panic(err)
    // }
	// println(string(out))
	// f.WriteString("; \n")

	start := strings.Split(string(cpuStart), " ")
	stop := strings.Split(string(cpuStop), " ")

	// println(len(s))

	// justString := strings.Join(s,";")
	// println(justString)

	// cpuCount := strings.Count(justString, "cpu")

	f.WriteString(";;User mode;")
	f.WriteString("Nice;")
	f.WriteString("System mode;")
	f.WriteString("Idle;")
	f.WriteString("IO wait;")
	f.WriteString("Hard Interrupt;")
	f.WriteString("Soft Interupt;")
	f.WriteString("Steal;")
	f.WriteString("Guest;")
	f.WriteString("Guest Nice;\n")

	startCPUString := string(start[5])
	stopCPUString := string(stop[5])

	println("IdelTimeStart " + startCPUString)
	println("IdelTimeStop " + stopCPUString)


	for counts:=0; counts < 2; counts ++{
		for i:=0; i<12; i++{
			if counts < 1{
				f.WriteString(start[i] + ";")
			}else{
				f.WriteString(stop[i] + ";")
			}
		}
		f.WriteString("\n")
	}
	f.WriteString("CPU Idle Diff \n")
		startCPU, err := strconv.ParseInt(startCPUString, 10, 64)
		if err != nil{
			panic(err)
		}
		stopCPU, err := strconv.ParseInt(stopCPUString, 10, 64)
		if err != nil{
			panic(err)
		}

		diff := stopCPU - startCPU

		diffstring := strconv.Itoa(int(diff))
		println("IdelTimeDiff " + diffstring)

		f.WriteString(diffstring + "; \n")
}
// ComputeRoundTripTime computes the RoundTripTime out of the SendTime and the ConsumeTime stored in arrays during the measurement
func ComputeRoundTripTime(sendTimes[100000] string, consumeTimes[100000]string, messages int) ([100000]string) {
	var roundTripTime [100000]string
	for i:=0; i<messages; i++{
		consumeTime, err := strconv.ParseInt(consumeTimes[i], 10, 64)
		if err != nil{
			panic(err)
		}
		sendTime, err := strconv.ParseInt(sendTimes[i], 10, 64)
		if err != nil{
			panic(err)
		}
		roundTrip := consumeTime - sendTime
		roundTripMS := float64(roundTrip) / float64(1000000) //Nanosekunden in Milisekunden
		roundTripTime[i] = strconv.FormatFloat(roundTripMS, 'f', 6, 64)
	}

	return roundTripTime
}
// ComputeConsumeTime is used to correct the consume time from containing the encoding- sending and consuetime to only contain the consumetime
func ComputeConsumeTime(EncodingTime[9][100000] string, SendTime[9][100000] string, ConsumeTime[9][100000] string, instances int, messages int)([9][100000]string){
	corrConsumeTime := ConsumeTime
	var encodingTime float64
	// var sendTime float64
	var err error

	for inst:=0; inst <= instances; inst++{
		for i:=0; i < messages; i++{

			// get values fo encodingTime and sendTime
				// if inst > 0{
				// 	encodingTime, err = strconv.ParseFloat(EncodingTime[inst -1][i], 64)
				// 	if err != nil{
				// 		panic(err)
				// 	}
				// }else{
					encodingTime, err = strconv.ParseFloat(EncodingTime[inst][i], 64)
					if err != nil{
						panic(err)
					}	
				// }
	
				// println(inst)
				consumeTime, err := strconv.ParseFloat(ConsumeTime[inst][i], 64)
				if err != nil{
					panic(err)
				}

			// measured consumeTime includes encoding- and sendTime, therefore we have to substract
			durationMs := consumeTime - encodingTime
			corrConsumeTime[inst][i] = strconv.FormatFloat(durationMs, 'f', 6, 64)
		}
	}

	return corrConsumeTime
}
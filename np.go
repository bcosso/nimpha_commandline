package main

import (
	"bytes"
	"bufio"
	"fmt"
	"log"
	// "errors"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"github.com/bcosso/rsocket_json_requests"
	"strconv"
	"strings"
)

type config struct {
	Folder_Path     string  `json:"path"`
	Peers           []peers `json:"peers"`
	Number_Replicas string  `json:"number_replicas"`
	Max_Heap_Size   string  `json:"max_heap_size"`
	Instance_name   string  `json:"instance_name"`
	Instance_port   string  `json:"instance_port"`
	Instance_ip     string  `json:"instance_ip"`
}

type peers struct {
	Ip   string `json:"ip"`
	Name string `json:"name"`
	Port string `json:"port"`
}

type index_table struct {
	Index_id   int         `json:"index_id"`
	Index_rows []index_row `json:"index_row"`
}

type index_row struct {
	Index_from    int    `json:"index_from"`
	Index_to      int    `json:"index_to"`
	Instance_name string `json:"instance_name"`
	Instance_ip   string `json:"instance_ip"`
	Instance_port string `json:"instance_port"`
	Table_name    string `json:"table_name"`
}

type mem_table struct {
	Key_id int       `json:"key_id"`
	Rows   []mem_row `json:"mem_row"`
}

type mem_row struct {
	Key_id     int    `json:"key_id"`
	Table_name string `json:"table_name"`
	Document   string `json:"document"`
}

var configs config

//Client Facing Methods //

func load_mem_table() {
	response, err := http.Get("http://" + configs.Instance_ip + ":" + configs.Instance_port + "/" + configs.Instance_name + "/load_mem_table/")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(response.Body)
}

func select_contains(querystring string) {
	var rows interface{}
	url := "http://" + configs.Instance_ip + ":" + configs.Instance_port + "/" + configs.Instance_name + "/select_data_where_worker_contains?" + querystring
	response, err := http.Get(url)
	fmt.Println(url)
	if err != nil {
		fmt.Println("Err")
		log.Fatal(err)
	}

	dec := json.NewDecoder(response.Body)
	dec.DisallowUnknownFields()

	err = dec.Decode(&rows)
	fmt.Println(rows)
}

func select_data(querystring string) {
	var rows interface{}
	url := "http://" + configs.Instance_ip + ":" + configs.Instance_port + "/" + configs.Instance_name + "/select_data?" + querystring
	response, err := http.Get(url)
	fmt.Println(url)
	if err != nil {
		fmt.Println("Err")
		log.Fatal(err)
	}

	dec := json.NewDecoder(response.Body)
	dec.DisallowUnknownFields()

	err = dec.Decode(&rows)
	fmt.Println(rows)
}

func main() {
	configfile, err := os.Open("configfile.json")
	if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()
	root, err := ioutil.ReadAll(configfile)
	json.Unmarshal(root, &configs)

	if len(os.Args)  < 2 {
		startShell()
		return
	}

	parse_rsock()
}

func parse() {

	switch os.Args[1] {
	case "load":
		load_mem_table()
		break
	case "select":
		select_data(os.Args[2])
		break
	case "select_contains":
		select_contains(os.Args[2])
		break
	case "start":
		cmd := exec.Command("nimpha.exe")
		err := cmd.Start()

		if err != nil {
			fmt.Println(err.Error())
			return
		}
		break
	case "":
		startShell()
		break
	}
	

}


func startShell(){
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("-------------------------------------------")
	fmt.Println("Nimpha Shell")
	fmt.Println("-------------------------------------------")

	for {
		fmt.Print("nimpha> ")
		text, _:= reader.ReadString('\n')
    // convert CRLF to LF
    	text = strings.Replace(text, "\n", "", -1)
		text = strings.Replace(text, "\r", "", -1)
		if strings.Index(text, "command") == 0 {
			testConcurrency()
		}else{
			arr := []string{"", "", text}
			query_data_rsock(arr, "")
		}
	}

}

func parse_rsock() {
	switch os.Args[1] {
	case "load":
		load_mem_table_rsock()
		break
	case "concurrency":
		testConcurrency()
		break
	case "select":
		select_data_rsock(os.Args)
		//fmt.Println(os.Args[2])
		break
	case "select_contains":
		select_contains_rsock(os.Args)
		break
	case "insert":
		insert_data_rsock(os.Args)
		break
	case "delete":
		delete_data_rsock(os.Args)
		break
	case "start":
		cmd := exec.Command("nimpha.exe")
		err := cmd.Start()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	case "query":
		query_data_rsock(os.Args, "")
		break
	}
}

func load_mem_table_rsock() {
	var jsonStr = `
	{
	"table":"any"
	}
	`
	//jsonStr = fmt.Sprintf(jsonStr, querystring[2], querystring[3], querystring[4])

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	//fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/load_mem_table", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)
}

func select_contains_rsock(querystring []string) {
	var jsonStr = `
{
"table":"%s",
"where_field":"%s",
"where_content":"%s"
}
`
	jsonStr = fmt.Sprintf(jsonStr, querystring[2], querystring[3], querystring[4])

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/select_data_where_worker_contains", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)
}

func select_data_rsock(querystring []string) {
	//var rows interface{}
	//url := "http://" + configs.Instance_ip + ":"  + configs.Instance_port + "/" + configs.Instance_name + "/select_data?" + querystring

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s",
	"where_operator":"%s"
	}
	`
	jsonStr = fmt.Sprintf(jsonStr, querystring[2], querystring[3], querystring[4], querystring[5])

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/select_data", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)

}

func insert_data_rsock(querystring []string) {
	//var rows interface{}
	//url := "http://" + configs.Instance_ip + ":"  + configs.Instance_port + "/" + configs.Instance_name + "/select_data?" + querystring

	configfile, err := os.Open(querystring[4])
	if err != nil {
		log.Fatal(err)
	}
	defer configfile.Close()

	root, err := ioutil.ReadAll(configfile)
	jsonMapBody := make(map[string]interface{})
	err = json.Unmarshal(root, &jsonMapBody)

	if err != nil {
		panic(err)
	}

	var jsonStr = `
	{
	"key_id":"%s",
	"table":"%s",
	"body":%s
	}
	`

	fmt.Println(querystring[4])

	jsonStr = fmt.Sprintf(jsonStr, querystring[2], querystring[3], root)

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err = json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	// result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/insert", jsonStr)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/insert_data", jsonStr)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)

}

func delete_data_rsock(querystring []string) {

	var jsonStr = `
	{
	"table":"%s",
	"where_field":"%s",
	"where_content":"%s",
	"where_operator":"%s"
	}
	`
	jsonStr = fmt.Sprintf(jsonStr, querystring[2], querystring[3], querystring[4], querystring[5])

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/delete_data_where", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)

}


func query_data_rsock(querystring []string, outputFile string) {
	//var rows interface{}
	//url := "http://" + configs.Instance_ip + ":"  + configs.Instance_port + "/" + configs.Instance_name + "/select_data?" + querystring

	var jsonStr = `
	{
	"query":"%s"
	}
	`

	// fmt.Println("tessssssst")
	jsonStr = fmt.Sprintf(jsonStr, strings.Join(querystring[2:], " "))

	// fmt.Println(jsonStr)
	// fmt.Println("tessssssst")
	jsonMap := make(map[string]interface{})
	fmt.Println("tttttttt")

	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Println(jsonMap)
	// fmt.Println(jsonMap)
	_port, erro := strconv.Atoi(configs.Instance_port)
	if erro != nil {
		fmt.Println(erro)
		panic(erro)
	}
	fmt.Println(_port)
	rsocket_json_requests.RequestConfigs("localhost", _port)
	fmt.Println(_port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/execute_query", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	var jsonMapResult []map[string]interface{}
	
	intermediate_inteface := result.([]interface{})
	json_rows_bytes, _ := json.Marshal(intermediate_inteface)
	
	//fmt.Println(intermediate_inteface)
	reader := bytes.NewReader(json_rows_bytes)

	dec := json.NewDecoder(reader)
	dec.DisallowUnknownFields()

	errDec := dec.Decode(&jsonMapResult)
	if errDec != nil {
		log.Fatal(errDec)
	}

	outputText := ""
	for _, rows := range jsonMapResult {

		// m is a map[string]interface.
		// loop over keys and values in the map.
		for name, document := range rows {
			if (name == "Rows"){
				m := document.(map[string]interface{})
				for k, v := range m {
					fmt.Println(k, ":", v)
					outputText += "\n" + string(k) + ":" + v.(string)
				}
			}
		}
	}
	if outputFile != ""{
		// d1 := []byte(outputText)
		f, err0 := os.Create(outputFile)
		if err0 != nil {
			fmt.Println(err0)
		}
	   
		n, err0 := f.WriteString(outputText)
		if err0 != nil {
			fmt.Println(err0)
		//  log.Fatal(err0)
		}
		fmt.Printf("wrote %d bytes\n", n)
		f.Sync()
	}
	// fmt.Println(result)

}

func testConcurrency(){
	fmt.Println("Concurrency Test")
	textQuery := []string{"select client_address, client_number from table3 where client_number < 10000", "select client_address, client_number from table3 where client_number > 91000"}
	arr := []string{"", "", textQuery[0]}
	arr2 := []string{"", "", textQuery[1]}
	go query_data_rsock(arr, "1")
	go query_data_rsock(arr2, "2")
}

//select table1.name_client from table1, tableadress where  table1.client_number > 1
//select table1.name_client from table1, tableadress where  table1.client_number > 1

// .\np.exe query "select table1.name_client, tableaddress.client_address from table1, tableaddress where table1.client_number = tableaddress.client_number and table1.client_number > 1"


// select tab1.client_address, tab2.client_name from table3 as tab1, tableclient as tab2 where tab1.client_number = tab2.client_number
// select tab1.client_address, tab2.client_name from tableclient as tab2, table3 as tab1 where tab1.client_number = tab2.client_number
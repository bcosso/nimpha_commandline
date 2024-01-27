package main

import (
	"fmt"
	"log"
	// "errors"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"rsocket_json_requests"
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
	}
}

func parse_rsock() {
	switch os.Args[1] {
	case "load":
		load_mem_table_rsock()
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
		query_data_rsock(os.Args)
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
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
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
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
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
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
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
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/insert", jsonStr)
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
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/delete_data_where", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)

}


func query_data_rsock(querystring []string) {
	//var rows interface{}
	//url := "http://" + configs.Instance_ip + ":"  + configs.Instance_port + "/" + configs.Instance_name + "/select_data?" + querystring

	var jsonStr = `
	{
	"query":"%s"
	}
	`
	jsonStr = fmt.Sprintf(jsonStr, strings.Join(querystring[2:], " "))

	fmt.Println(jsonStr)

	jsonMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &jsonMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(jsonMap)
	_port, _ := strconv.Atoi(configs.Instance_port)
	rsocket_json_requests.RequestConfigs("127.0.0.1", _port)
	result, err1 := rsocket_json_requests.RequestJSON("/"+configs.Instance_name+"/execute_query", jsonMap)
	if err1 != nil {
		fmt.Println(err1)
	}
	fmt.Println(result)

}
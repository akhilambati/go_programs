package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type QueryResponse struct {
	Timing Timing `json:"timing"`
	Root   Root   `json:"root"`
}

type Timing struct {
	QueryTime        float64 `json:"querytime"`
	SummaryFetchTime float64 `json:"summaryfetchtime"`
	SearchTime       float64 `json:"searchtime"`
}

type Root struct {
	ID        string           `json:"id"`
	Relevance int              `json:"relevance"`
	Fields    Fields           `json:"feilds"`
	Coverage  Coverage         `json:"coverage"`
	Children  []ChildrenObject `json:"children"`
}

type Fields struct {
	TotalCount int64 `json:"totalCount"`
	Count      int64 `json:"count()"`
}

type Coverage struct {
	Coverage    int  `json:"coverage"`
	Documents   int  `json:"documents"`
	Full        bool `json:"full"`
	Nodes       int  `json:"nodes"`
	Results     int  `json:"results"`
	ResultsFull int  `json:"resultsFull"`
}

type ChildrenObject struct {
	ID        string           `json:"id"`
	Relevance int              `json:"relevance"`
	Label     string           `json:"label"`
	Value     string           `json:"value"`
	Children  []ChildrenObject `json:"children"`
	Fields    Fields           `json:"fields"`
}

const search_url string = "http://localhost:8080/search"
const attr string = "attribute"
const query string = "select * from %s where true | all( group(%s) each(output(count())))"

var header []string = []string{"Field", "Count"}

func checkArgs() []string {
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Printf("Required arguments %d, but given arguments %d", 3, 0)
		panic("No of arguments provided less than what is required")
	}
	return args
}

func doPostRequest(query string) int64 {
	requestBody := []byte(`{"hits":0,"yql":` + query + `}`)
	req, err := http.NewRequest("POST", search_url, bytes.NewBuffer(requestBody))
	if err != nil {
		panic("Error while submitting request for search query of " + query)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		panic("Error while sending client request")
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic("Error while parsing response from post query")
	}
	var queryResponse QueryResponse
	err_unmarshall := json.Unmarshal(body, &queryResponse)
	if err_unmarshall != nil {
		panic("Error while unmarshalling response")
	}
	var fieldCount int64 = 0
	for _, grouped_data := range queryResponse.Root.Children[0].Children[0].Children {
		fieldCount += grouped_data.Fields.Count
	}
	return fieldCount
}

func doGetAttributeFieldCount(attrField string, schemaName string) int64 {
	result_query := fmt.Sprintf(query, schemaName, attrField)
	return doPostRequest(result_query)
}

func getAttributeCount(attrLine string, schemaName string) (string, int64) {
	tokens := strings.Split(attrLine, "\\s+")
	gotRequiredField := false
	for _, value := range tokens {
		if value == "field" {
			gotRequiredField = true
		} else {
			if gotRequiredField {
				fieldName := strings.TrimSpace(value)
				return fieldName, doGetAttributeFieldCount(fieldName, schemaName)
			}
		}
	}

	return "", -1
}

func main1() {
	var url string = "https://doc-search.vespa.oath.cloud/search/?yql=select%20*%20from%20purchase%20where%20true%20%7C%20all(%20group(customer)%20each(output(count()))%20)&hits=0"
	response, err := http.Get(url)
	if err != nil {
		panic("Error while submitting request for search query of " + query)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))
	var queryResponse QueryResponse
	err_p := json.Unmarshal(body, &queryResponse)
	fmt.Println(err_p != nil)
	fmt.Println(queryResponse.Root.Children[0].Children[0].Children)
}

func main() {
	args := checkArgs()

	file_path := args[0]
	schema_fields_count_file_path := args[1]
	schema_name := args[2]

	file, err := os.Open(file_path)
	if err != nil {
		fmt.Printf("Error opening file %s", file_path)
		panic("Error while opening file " + file_path)
	}

	schema_fields_count_file, err := os.Create(schema_fields_count_file_path)
	if err != nil {
		fmt.Printf("Error while creating file " + schema_fields_count_file_path)
		panic("Error while creating file " + schema_fields_count_file_path)
	}

	writer := csv.NewWriter(schema_fields_count_file)

	defer writer.Flush()
	defer file.Close()

	writer.Write(header)

	previous_line := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, attr) {
			field, fieldCount := getAttributeCount(previous_line, schema_name)
			writer.Write([]string{field, strconv.Itoa(int(fieldCount))})
		}
		previous_line = line

	}

}

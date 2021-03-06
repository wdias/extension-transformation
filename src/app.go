package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/kataras/iris"
	"github.com/kataras/iris/context"
)

var functionMap = map[string]string{
	"AggregateAccumulative": "aggregate-accumulative",
}

// Timeseries : Timeseries structure with timeseriesId and metadataIds
type Timeseries struct {
	TimeseriesID   string `json:"timeseriesId"`
	ModuleID       string `json:"moduleId"`
	ValueType      string `json:"valueType"`
	ParameterID    string `json:"parameterId"`
	LocationID     string `json:"locationId"`
	TimeseriesType string `json:"timeseriesType"`
	TimeStepID     string `json:"timeStepId"`
}

// Variable : Variable
type Variable struct {
	VariableID string     `json:"variableId"`
	Timeseries Timeseries `json:"timeseries"`
}

// Extension blabla
type Extension struct {
	ExtensionID string `json:"extensionId"`
	Extension   string `json:"extension"`
	Function    string `json:"function"`
	Data        struct {
		InputVariables  []string   `json:"inputVariables"`
		OutputVariables []string   `json:"outputVariables"`
		Variables       []Variable `json:"variables"`
	} `json:"data"`
	Options json.RawMessage `json:"options"`
}

// Point : Data Point
type Point struct {
	Time  string  `json:"time"`
	Value float64 `json:"value"`
}

// DataVariable : DataVariable
type DataVariable struct {
	Variable
	Data []Point `json:"data"`
}

// FunctionParams : Function parameters
type FunctionParams struct {
	ExtensionID     string          `json:"extensionId"`
	Extension       string          `json:"extension"`
	Function        string          `json:"function"`
	InputVariables  []DataVariable  `json:"inputVariables"`
	OutputVariables []Variable      `json:"outputVariables"`
	Options         json.RawMessage `json:"options"`
	Callback        string          `json:"callback"`
}

// FunctionResponse : Function response
type FunctionResponse struct {
	OutputVariables []DataVariable `json:"outputVariables"` // NOTE: Need to be at top
	FunctionParams
}

func generateToken() string {
	return fmt.Sprintf("T%08d", rand.Intn(100000000))
}

func getVariableByIDs(variableList []Variable, variableIDs []string) (variables []Variable) {
	for _, variableID := range variableIDs {
		for _, variable := range variableList {
			if variable.VariableID == variableID {
				variables = append(variables, variable)
			}
		}
	}
	return
}

func getDataPoints(variable Variable, queryOptions string) (points []Point, err error) {
	fmt.Println("GetDataPoints:", variable, queryOptions)
	adapterHost := fmt.Sprint("http://adapter-", strings.ToLower(variable.Timeseries.ValueType), ".default.svc.cluster.local")
	response, err := netClient.Get(fmt.Sprint(adapterHost, "/timeseries/", variable.Timeseries.TimeseriesID, "?", queryOptions))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		fmt.Println("GetDataPoints Unable to find Timeseries:", string(body))
		return nil, fmt.Errorf("Unable to find Timeseries: %q", variable.Timeseries.TimeseriesID)
	}
	err = json.Unmarshal(body, &points)
	return
}

func saveDataPoints(variable DataVariable) (err error) {
	fmt.Println("SaveDataPoints:", variable)
	adapterHost := fmt.Sprint("http://adapter-", strings.ToLower(variable.Timeseries.ValueType), ".default.svc.cluster.local")
	jsonValue, _ := json.Marshal(variable.Data)
	response, err := netClient.Post(fmt.Sprint(adapterHost, "/timeseries/", variable.Timeseries.TimeseriesID), "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("Unable to find Timeseries for save data: %q", variable.Timeseries.TimeseriesID)
	}
	return
}

func getFunctionParams(extension *Extension, queryOptions string) (functionParams *FunctionParams, err error) {
	var inputVariables = getVariableByIDs(extension.Data.Variables, extension.Data.InputVariables)
	var inputVariablesWithData []DataVariable
	for _, inputVariable := range inputVariables {
		points, err := getDataPoints(inputVariable, queryOptions)
		if err != nil {
			return nil, fmt.Errorf("Unable to get data for Timeseries: %q", inputVariable.Timeseries.TimeseriesID)
		}
		var inputVariableWithData DataVariable
		inputVariableWithData.VariableID = inputVariable.VariableID
		inputVariableWithData.Timeseries = inputVariable.Timeseries
		inputVariableWithData.Data = points
		inputVariablesWithData = append(inputVariablesWithData, inputVariableWithData)
	}
	var outputVariables = getVariableByIDs(extension.Data.Variables, extension.Data.OutputVariables)

	functionParams = &FunctionParams{}
	functionParams.ExtensionID = extension.ExtensionID
	functionParams.Extension = extension.Extension
	functionParams.Function = extension.Function
	functionParams.InputVariables = inputVariablesWithData
	functionParams.OutputVariables = outputVariables
	functionParams.Options = extension.Options
	functionParams.Callback = "http://extension-transformation.default.svc.cluster.local/extension/transformation/callback"
	return
}

func triggerFunction(functionParams *FunctionParams, token string, queryOptions string) (err error) {
	transformationHost := fmt.Sprint("http://transformation-", functionMap[functionParams.Function], ".default.svc.cluster.local")
	urlQuery := fmt.Sprint("?token=", token, "&", queryOptions)
	transformationURL := fmt.Sprint(transformationHost, "/extension/transformation/", functionMap[functionParams.Function], urlQuery)
	fmt.Println("Trigger function:", transformationURL, functionParams)
	jsonValue, _ := json.Marshal(functionParams)
	response, err := netClient.Post(transformationURL, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		fmt.Println("Unable to trigger Extension: ", string(body))
		return fmt.Errorf("Unable to trigger Extension: %q-%q", functionParams.Extension, functionMap[functionParams.Function])
	}
	return nil
}

var tr = &http.Transport{
	MaxIdleConns:       10,
	IdleConnTimeout:    30 * time.Second,
	DisableCompression: true,
	Dial: (&net.Dialer{
		Timeout: 5 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 5 * time.Second,
}
var netClient = &http.Client{
	Transport: tr,
	Timeout:   time.Second * 10,
}

func main() {
	app := iris.Default()
	app.Post("/extension/transformation/trigger/{extensionID:string}", func(ctx iris.Context) {
		extensionID := ctx.Params().Get("extensionID")
		fmt.Println("Trigger ExtensionID:", extensionID)
		extension := &Extension{}
		err := ctx.ReadJSON(extension)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		queryOptions := ctx.Request().URL.RawQuery
		token := generateToken()
		fmt.Println("Extension:", extension)
		funcParams, err := getFunctionParams(extension, queryOptions)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		fmt.Println("FunctionParams:", funcParams)
		err = triggerFunction(funcParams, token, queryOptions)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		ctx.JSON(iris.Map{
			"message": "OK",
		})
	})

	app.Post("/extension/transformation/callback/{token:string}", func(ctx iris.Context) {
		token := ctx.Params().Get("token")
		fmt.Println("Trigger Callback Token:", token)
		funcResp := &FunctionResponse{}
		err := ctx.ReadJSON(funcResp)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		fmt.Println("FunctionResponse:", funcResp)
		for _, outputVariable := range funcResp.OutputVariables {
			err := saveDataPoints(outputVariable)
			if err != nil {
				ctx.JSON(context.Map{"response": err.Error()})
				return
			}
		}
		ctx.JSON(iris.Map{
			"message": "OK",
		})
	})

	app.Get("/public/hc", func(ctx iris.Context) {
		ctx.JSON(iris.Map{
			"message": "OK",
		})
	})
	// listen and serve on http://0.0.0.0:8080.
	app.Run(iris.Addr(":8080"))
}

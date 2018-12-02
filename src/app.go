package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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

// InputVariable : InputVariable
type InputVariable struct {
	Variable
	Data []Point `json:"data"`
}

// FunctionParams : Function parameters
type FunctionParams struct {
	ExtensionID     string          `json:"extensionId"`
	Extension       string          `json:"extension"`
	Function        string          `json:"function"`
	InputVariables  []InputVariable `json:"inputVariables"`
	OutputVariables []Variable      `json:"outputVariables"`
	Options         json.RawMessage `json:"options"`
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

func getDataPoints(variable Variable) (points []Point, err error) {
	fmt.Println("GetDataPoints:", variable)
	adapterHost := fmt.Sprint("http://adapter-", strings.ToLower(variable.Timeseries.ValueType), ".default.svc.cluster.local")
	response, err := netClient.Get(fmt.Sprint(adapterHost, "/timeseries/", variable.Timeseries.TimeseriesID))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("Unable to find Timeseries: %q", variable.Timeseries.TimeseriesID)
	}
	err = json.Unmarshal(body, &points)
	return
}

func getFunctionParams(extension *Extension) (functionParams *FunctionParams, err error) {
	var inputVariables = getVariableByIDs(extension.Data.Variables, extension.Data.InputVariables)
	var inputVariablesWithData []InputVariable
	for _, inputVariable := range inputVariables {
		points, err := getDataPoints(inputVariable)
		if err != nil {
			return nil, fmt.Errorf("Unable to get data for Timeseries: %q", inputVariable.Timeseries.TimeseriesID)
		}
		var inputVariableWithData InputVariable
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
	return
}

func triggerFunction(functionParams *FunctionParams) (err error) {
	transformationHost := fmt.Sprint("http://transformation-", functionMap[functionParams.Function], ".default.svc.cluster.local")
	transformationURL := fmt.Sprint(transformationHost, "/extension/transformation/", functionMap[functionParams.Function])
	fmt.Println("Trigger function:", transformationURL, functionParams)
	jsonValue, _ := json.Marshal(functionParams)
	response, err := netClient.Post(transformationURL, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	// body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
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
		fmt.Println("extensionID:", extensionID)
		extension := &Extension{}
		err := ctx.ReadJSON(extension)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		fmt.Println("Extension:", extension)
		funcParams, err := getFunctionParams(extension)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
		}
		fmt.Println("FunctionParams:", funcParams)
		err = triggerFunction(funcParams)
		if err != nil {
			ctx.JSON(context.Map{"response": err.Error()})
			return
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
